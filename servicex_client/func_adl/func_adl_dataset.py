# Copyright (c) 2023, IRIS-HEP
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
from typing import Union, Optional, Any, TypeVar

from func_adl import EventDataset
from servicex_client.configuration import Configuration
from servicex_client.dataset_identifier import DataSetIdentifier, FileListDataset
from servicex_client.dataset import Dataset
from servicex_client.query_cache import QueryCache
from servicex_client.servicex_adapter import ServiceXAdapter

T = TypeVar("T")


class FuncADLDataset(Dataset, EventDataset[T]):
    async def execute_result_async(self, a: ast.AST, title: Optional[str] = None) -> Any:
        pass

    def check_data_format_request(self, f_name: str):
        pass

    def __init__(self, dataset_identifier: Union[
        DataSetIdentifier, FileListDataset],
                 sx_adapter: ServiceXAdapter = None,
                 title: str = "ServiceX Client",
                 codegen: str = None,
                 config: Configuration = None,
                 query_cache: QueryCache = None
                 ):
        super().__init__(dataset_identifier=dataset_identifier,
                         title=title,
                         codegen=codegen,
                         sx_adapter=sx_adapter,
                         config=config,
                         query_cache=query_cache)
