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
import logging
from abc import ABC
from concurrent.futures import ThreadPoolExecutor
from time import sleep
from typing import TypeVar, Any, cast, List, Optional, Union

import rich
from qastle import python_ast_to_text_ast

from func_adl import EventDataset, ObjectStream
from servicex_client.dataset_identifier import DataSetIdentifier, FileListDataset
from servicex_client.func_adl.util import has_tuple, FuncADLServerException, has_col_names
from servicex_client.minio_adpater import MinioAdapter
from servicex_client.models import TransformRequest, ResultDestination, ResultFormat
from servicex_client.servicex_adapter import ServiceXAdapter

T = TypeVar("T")


class ServiceXDatasetSourceBase(EventDataset[T], ABC):
    """
    Base class for a ServiceX backend dataset.

    While no methods are abstract, base classes will need to add arguments
    to the base `EventDataset` to make sure that it contains the information
    backends expect!
    """

    # How we map from func_adl to a servicex query
    _ds_map = {
        "ResultTTree": "get_data_rootfiles_async",
        "ResultParquet": "get_data_parquet_async",
        "ResultPandasDF": "get_data_pandas_df_async",
        "ResultAwkwardArray": "get_data_awkward_async",
    }

    # If it comes down to format, what are we going to grab?
    _format_map = {
        "root-file": "get_data_rootfiles_async",
        "parquet": "get_data_parquet_async",
    }

    # These are methods that are translated locally
    _execute_locally = ["ResultPandasDF", "ResultAwkwardArray"]

    # If we have a choice of formats, what can we do, in
    # prioritized order?
    _format_list = ["parquet", "root-file"]

    def __init__(
        self,
        dataset_identifier: Union[
                DataSetIdentifier, FileListDataset],
            title:str = "ServiceX Client",
            codegen:str = None,
            sx_adapter: ServiceXAdapter = None
    ):
        """
        Create a servicex dataset sequence from a servicex dataset
        """
        super().__init__(item_type=Any)
        self.minio = None
        self.minio_secret_key = None
        self.minio_access_key = None
        self.minio_secured = None
        self.minio_endpoint = None
        self.files_failed = None
        self.files_completed = None
        self.dataset_identifier = dataset_identifier
        self.servicex = sx_adapter
        self._return_qastle = True

        self.codegen = codegen
        self.title = title
        self.request_id = None

    @property
    def transform_request(self):
        sx_request = TransformRequest(
            title=self.title,
            codegen=self.codegen,
            result_destination=ResultDestination.object_store,
            result_format=self.result_format,
            selection=self.generate_qastle(self.query_ast)
        )
        self.dataset_identifier.populate_transform_request(sx_request)
        return sx_request

    async def monitor_status(self):
        while True:
            status = await self.servicex.get_transform_status(self.request_id)
            if not self.minio:
                self.minio = MinioAdapter(
                    endpoint_host=status['minio-endpoint'],
                    secure=status['minio-secured'],
                    access_key=status['minio-access-key'],
                    secret_key=status['minio-secret-key'],
                    bucket=status['request_id']
                )

            if status["status"] == "Complete":
                self.files_completed = status['files-completed']
                self.files_failed = status['files-failed']
                return

            print(status)
            sleep(20)

    def submit(self) -> concurrent.futures.Future:
        self.result_format = ResultFormat.parquet
        sx_request = self.transform_request
        self.request_id = self.servicex.submit_transform(sx_request)
        asyncio.run(self.monitor_status())

    def as_pandas(self):
        self.result_format = ResultFormat.parquet
        sx_request = self.transform_request
        self.request_id = self.servicex.submit_transform(sx_request)
        print("Request running ", self.request_id)

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