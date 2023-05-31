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
import asyncio
import logging
from typing import Union

import rich

from servicex_client.configuration import Configuration
from servicex_client.dataset_identifier import DataSetIdentifier, FileListDataset
from servicex_client.servicex_adapter import ServiceXAdapter
from servicex_client.func_adl.servicex_func_adl_uproot import ServiceXFuncADLUproot


class ServiceXClient:
    def __init__(self, backend=None, url=None, config_path=None):
        self.config = Configuration.read(config_path)
        self.endpoints = self.config.endpoint_dict()

        if bool(url) == bool(backend):
            raise ValueError("Only specify backend or url... not both")

        if url:
            self.servicex = ServiceXAdapter(url)
        elif backend:
            if backend not in self.endpoints:
                raise ValueError(f"Backend {backend} not defined in .servicex file")
            self.servicex = ServiceXAdapter(self.endpoints[backend].endpoint,
                                            refresh_token=self.endpoints[backend].token)

        # Cache available code generators
        self.code_generators = set(self.get_code_generators().keys())

    async def get_transforms_async(self):
        return self.servicex.get_transforms()

    def get_transforms(self):
        return asyncio.run(self.servicex.get_transforms())

    def get_transform_status(self, transform_id):
        return asyncio.run(self.servicex.get_transform_status(request_id=transform_id))

    async def get_transform_status_async(self, transform_id):
        return await self.servicex.get_transform_status(request_id=transform_id)

    def get_code_generators(self):
        return self.servicex.get_code_generators()

    def func_adl_uproot_dataset(self,
                                dataset_identifier: Union[
                                    DataSetIdentifier, FileListDataset],
                                title: str = "ServiceX Client",
                                codegen: str = "uproot"
                                ):
        if codegen not in self.code_generators:
            raise NameError(f"{codegen} code generator not supported by serviceX deployment at {self.servicex.url}")

        return ServiceXFuncADLUproot(dataset_identifier, sx_adapter=self.servicex,
                                     title=title, codegen=codegen, config=self.config)


