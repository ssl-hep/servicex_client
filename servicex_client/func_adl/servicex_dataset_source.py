# Copyright (c) 2022, IRIS-HEP
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.
#
# * Neither the name of the copyright holder nor the names of its
#   contributors may be used to endorse or promote products derived from
#   this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
import ast
import asyncio
import concurrent.futures
import copy
import logging
import os.path
from abc import ABC
from asyncio import Task, CancelledError
from pathlib import Path

import pandas
import typing
from rich.progress import Progress, TaskID, TextColumn, BarColumn, MofNCompleteColumn, \
    TimeRemainingColumn
from typing import TypeVar, Any, cast, List, Optional, Union

import rich
from qastle import python_ast_to_text_ast
from rich.table import Table
from tinydb import TinyDB

from func_adl import EventDataset, ObjectStream
from servicex_client.configuration import Configuration
from servicex_client.dataset_identifier import DataSetIdentifier, FileListDataset
from servicex_client.func_adl.util import has_tuple, FuncADLServerException, has_col_names
from servicex_client.minio_adpater import MinioAdapter
from servicex_client.models import TransformRequest, ResultDestination, ResultFormat, \
    Status
from servicex_client.query_cache import QueryCache
from servicex_client.servicex_adapter import ServiceXAdapter

T = TypeVar("T")


class ServiceXDatasetSourceBase(EventDataset[T], ABC):
    # These are methods that are translated locally
    _execute_locally = ["ResultPandasDF", "ResultAwkwardArray"]

    def __init__(
        self,
        dataset_identifier: Union[
                DataSetIdentifier, FileListDataset],
            title: str = "ServiceX Client",
            codegen: str = None,
            sx_adapter: ServiceXAdapter = None,
            config: Configuration = None,
            query_cache: QueryCache = None,
            servicex_polling_interval: int = 10,
            minio_polling_interval: int = 5):
        super().__init__(item_type=Any)
        self.servicex = sx_adapter
        self.configuration = config
        self.cache = query_cache

        self.dataset_identifier = dataset_identifier
        self.codegen = codegen
        self.title = title

        self.result_format = None
        self.signed_urls = False
        self.current_status = None
        self.download_path = None
        self.minio = None
        self.files_failed = None
        self.files_completed = None
        self._return_qastle = True

        self.request_id = None

        # Number of seconds in between ServiceX status polls
        self.servicex_polling_interval = servicex_polling_interval
        self.minio_polling_interval = minio_polling_interval

    def clone_with_new_ast(self, new_ast: ast.AST, new_type: typing.Any):
        """
        Override the method from ObjectStream - We need to be careful because the query
        cache is a tinyDB database that holds an open file pointer. We are not allowed
        to clone an open file handle, so for this property we will copy by reference
        and share it between the clones. Turns out ast class is also picky about copies,
        so we set that explicitly.
        :param new_ast:
        :param new_type:
        :return:
        """
        clone = copy.copy(self)
        for attr, value in vars(self).items():
            if type(value) == QueryCache:
                setattr(clone, attr, value)
            elif attr == "_q_ast":
                setattr(clone, attr, new_ast)
            else:
                setattr(clone, attr, copy.deepcopy(value))

        clone._item_type = new_type
        return clone

    @property
    def transform_request(self):
        if not self.result_format:
            raise ValueError("Unable to determine the result file format. Use set_result_format method")

        sx_request = TransformRequest(
            title=self.title,
            codegen=self.codegen,
            result_destination=ResultDestination.object_store,
            result_format=self.result_format,
            selection=self.generate_qastle(self.query_ast)
        )
        # Transfer the DID into the transform request
        self.dataset_identifier.populate_transform_request(sx_request)
        return sx_request

    def set_result_format(self, result_format: ResultFormat):
        self.result_format = result_format
        return self

    async def submit_and_download(self, signed_urls_only: bool = False):
        """
        Submit the transform request to ServiceX. Poll the transform status to see when
        the transform completes and to get the number of files in the dataset along with
        current progress and failed file count.
        :return:
        """
        download_files_task = None
        loop = asyncio.get_running_loop()

        def transform_complete(task: Task):
            """
            Called when the Monitor task completes. This could be because of exception or
            the transform completed
            :param task:
            :return:
            """
            if task.exception():
                rich.print("ServiceX Exception", task.exception())
                if download_files_task:
                    download_files_task.cancel("Transform failed")
                raise task.exception()

            if self.current_status.files_failed:
                rich.print(f"[bold red]Transforms completed with failures[/bold red] {self.current_status.files_failed} files failed out of {self.current_status.files}")
            else:
                rich.print("Transforms completed successfully")

        sx_request = self.transform_request

        # Let's see if this is in the cache already
        cached_record = self.cache.get_transform_by_hash(sx_request.compute_hash())

        if cached_record:
            rich.print("Returning results from cache")
            return cached_record.file_uris

        with Progress(
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                MofNCompleteColumn(),
                TimeRemainingColumn(compact=True, elapsed_when_finished=True),
        ) as progress:
            transform_progress = progress.add_task("Transform", start=False, total=None)

            minio_progress_bar_title = "Download" if not signed_urls_only else "Signing URLS"
            download_progress = progress.add_task(minio_progress_bar_title, start=False, total=None)

            self.request_id = await self.servicex.submit_transform(sx_request)

            monitor_task = loop.create_task(self.transform_status_listener(progress, transform_progress, download_progress))
            monitor_task.add_done_callback(transform_complete)

            download_files_task = loop.create_task(self.download_files(signed_urls_only,
                                                                       progress, download_progress))

            try:
                downloaded_files = await download_files_task

                # Update the cache
                self.cache.cache_transform(sx_request, self.current_status,
                                           self.download_path.as_posix(),
                                           downloaded_files)
                return downloaded_files
            except CancelledError:
                rich.print_json("Aborted file downloads due to transform failure")

    async def transform_status_listener(self, progress: Progress,
                                        progress_task: TaskID, download_task: TaskID):
        """
        Poll ServiceX for the status of a transform. Update progress bars and keep track
        of status. Once we know the number of files in the dataset, update the progress
        bars.
        """

        # Actual number of files in the dataset. We only know this once the DID
        # finder has completed its work. In the meantime transformers will already
        # start up and begin work on the files we know about
        final_count = None

        while True:
            s = await self.servicex.get_transform_status(self.request_id)

            # Is this the first time we've polled status? We now know the request ID.
            # Update the display and set our download directory.
            if not self.current_status:
                rich.print(f"[bold]ServiceX Transform {s.request_id}[/bold]")
                self.download_path = self.cache.cache_path_for_transform(s)

            self.current_status = s

            # Do we finally know the final number of files in the dataset? Now is the
            # time to properly initialize the progress bars
            if not final_count and self.current_status.files:
                final_count = self.current_status.files
                progress.update(progress_task, total=final_count)
                progress.start_task(progress_task)

                progress.update(download_task, total=final_count)
                progress.start_task(download_task)

            progress.update(progress_task, completed=self.current_status.files_completed)

            # We can only initialize the minio adapter with data from the transform
            # status. This includes the minio host and credentials. We use the
            # transform id as the bucket.
            if not self.minio:
                self.minio = MinioAdapter.for_transform(self.current_status)

            if self.current_status.status == Status.complete:
                self.files_completed = self.current_status.files_completed
                self.files_failed = self.current_status.files_failed
                return

            await asyncio.sleep(self.servicex_polling_interval)

    async def download_files(self, signed_urls_only: bool, progress: Progress, download_progress: TaskID):
        """
        Task to monitor the list of files in the transform output's bucket. Any new files
        will be downloaded.
        """
        files_seen = set()
        downloaded_file_paths = []
        download_tasks = []
        loop = asyncio.get_running_loop()

        async def download_file(minio: MinioAdapter, filename: str,  progress: Progress, download_progress: TaskID):
            await minio.download_file(filename, self.download_path)
            downloaded_file_paths.append(os.path.join(self.download_path, filename))
            progress.advance(download_progress)

        async def get_signed_url(minio: MinioAdapter, filename: str,  progress: Progress, download_progress: TaskID):
            url = await minio.get_signed_url(filename)
            downloaded_file_paths.append(url)
            progress.advance(download_progress)

        while True:
            await asyncio.sleep(self.minio_polling_interval)
            if self.minio:
                files = await self.minio.list_bucket()
                for file in files:
                    if file.filename not in files_seen:
                        if signed_urls_only:
                            download_tasks.append(
                                loop.create_task(get_signed_url(self.minio, file.filename, progress, download_progress))
                            )
                        else:
                            download_tasks.append(
                                loop.create_task(download_file(self.minio, file.filename,
                                                               progress, download_progress)))
                        files_seen.add(file.filename)

            # Once the transform is complete we can stop polling since all of the files
            # are guaranteed to be in the bucket.
            if self.current_status and self.current_status.status == Status.complete:
                break

        # Now just wait until all of our tasks complete
        await asyncio.gather(*download_tasks)
        return downloaded_file_paths

    async def as_parquet_files(self):
        self.result_format = ResultFormat.parquet
        return await self.submit_and_download()

    async def as_root_files(self):
        self.result_format = ResultFormat.root_file
        return await self.submit_and_download()

    async def as_pandas(self):
        parquet_files = await self.as_parquet_files()
        dataframes = [pandas.read_parquet(p) for p in parquet_files]
        return dataframes

    async def as_signed_urls(self):
        return await self.submit_and_download(signed_urls_only=True)

    def generate_qastle(self, a: ast.AST) -> str:
        """Generate the qastle from the ast of the query.

        1. The top level function is already marked as being "ok"
        1. If the top level function is something we have to process locally,
           then we strip it off.

        Args:
            a (ast.AST): The complete AST of the request.

        Returns:
            str: Qastle that should be sent to servicex
        """
        top_function = cast(ast.Name, a.func).id
        source = a
        if top_function in self._execute_locally:
            # Request the default type here
            default_format = self._ds.first_supported_datatype(["parquet", "root-file"])
            assert default_format is not None, "Unsupported ServiceX returned format"
            method_to_call = self._format_map[default_format]

            stream = a.args[0]
            col_names = a.args[1]
            if method_to_call == "get_data_rootfiles_async":
                # If we have no column names, then we must be using a dictionary to set them - so just pass that
                # directly.
                assert isinstance(
                    col_names, (ast.List, ast.Constant, ast.Str)
                ), f"Programming error - type name not known {type(col_names).__name__}"
                if isinstance(col_names, ast.List) and len(col_names.elts) == 0:
                    source = stream
                else:
                    source = ast.Call(
                        func=ast.Name(id="ResultTTree", ctx=ast.Load()),
                        args=[
                            stream,
                            col_names,
                            ast.Str("treeme"),
                            ast.Str("junk.root"),
                        ],
                    )
            elif method_to_call == "get_data_parquet_async":
                source = stream
                # See #32 for why this is commented out
                # source = ast.Call(
                #     func=ast.Name(id='ResultParquet', ctx=ast.Load()),
                #     args=[stream, col_names, ast.Str('junk.parquet')])
            else:  # pragma: no cover
                # This indicates a programming error
                assert False, f"Do not know how to call {method_to_call}"

        elif top_function == "ResultParquet":
            # Strip off the Parquet function, do a select if there are arguments for column names
            source = a.args[0]
            col_names = cast(ast.List, a.args[1]).elts

            def encode_as_tuple_reference(c_names: List) -> List[ast.AST]:
                # Encode each column ref as a index into the tuple we are being passed
                return [
                    ast.Subscript(
                        value=ast.Name(id="x", ctx=ast.Load()),
                        slice=ast.Constant(idx),
                        ctx=ast.Load(),
                    )
                    for idx, _ in enumerate(c_names)
                ]

            def encode_as_single_reference():
                # Single reference for a bare (non-col) variable
                return [
                    ast.Name(id="x", ctx=ast.Load()),
                ]

            if len(col_names) > 0:
                # Add a select on top to set the column names
                if len(col_names) == 1:
                    # Determine if they built a tuple or not
                    values = (
                        encode_as_tuple_reference(col_names)
                        if has_tuple(source)
                        else encode_as_single_reference()
                    )
                elif len(col_names) > 1:
                    values = encode_as_tuple_reference(col_names)
                else:
                    assert False, "make sure that type checkers can figure this out"

                d = ast.Dict(keys=col_names, values=values)
                tup_func = ast.Lambda(
                    args=ast.arguments(args=[ast.arg(arg="x")]), body=d
                )
                c = ast.Call(
                    func=ast.Name(id="Select", ctx=ast.Load()),
                    args=[source, tup_func],
                    keywords=[],
                )
                source = c

        return python_ast_to_text_ast(source)

    async def execute_result_async(
        self, a: ast.Call, title: Optional[str] = None
    ) -> Any:
        r"""
        Run a query against a func-adl ServiceX backend. The appropriate part of the AST is
        shipped there, and it is interpreted.

        Arguments:

            a:                  The ast that we should evaluate
            title:              Optional title to be added to the transform

        Returns:
            v                   Whatever the data that is requested (awkward arrays, etc.)
        """
        # Check the call is legal for this datasource.
        a_func = cast(ast.Name, a.func)

        # Get the qastle string for this query
        q_str = self.generate_qastle(a)
        logging.getLogger(__name__).debug(f"Qastle string sent to servicex: {q_str}")

        # If only qastle is wanted, return here.
        if self._return_qastle:
            return q_str

        # Find the function we need to run against.
        if a_func.id in self._ds_map:
            name = self._ds_map[a_func.id]
        else:
            data_type = self._ds.first_supported_datatype(["parquet", "root-file"])
            if data_type is not None and data_type in self._format_map:
                name = self._format_map[data_type]
            else:
                raise FuncADLServerException(
                    f"Internal error - asked for {a_func.id} - but this dataset does not support it."
                )

        # Run the query for real!
        attr = getattr(self._ds, name)
        result = await attr(q_str, title=title)

        # If this is a single column awkward query, and the user did not specify a column name, then
        # we will return the first column.
        if (
            "awkward" in name
            and (not has_col_names(a))
            and 'key="col1"' in str(result.layout)
        ):
            result = result["col1"]

        return result

    def as_qastle(self):
        return self.value()