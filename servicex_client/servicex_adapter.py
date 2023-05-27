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
import json
from typing import Union

import httpx
import requests
import rich

from servicex_client.dataset_identifier import DataSetIdentifier, FileListDataset
from servicex_client.models import TransformRequest, TransformStatus


class ServiceXAdapter:
    def __init__(self, url: str):
        self.url = url

    async def get_transforms(self):
        async with httpx.AsyncClient() as client:
            r = await client.get(url=f"{self.url}/servicex/transformation")
            statuses = [TransformStatus(**status) for status in r.json()['requests']]
        return statuses

    def get_code_generators(self):
        r = requests.get(url=f"{self.url}/multiple-codegen-list")
        return r.json()

    def submit_transform(self, transform_request: TransformRequest):
        r = requests.post(url=f"{self.url}/servicex/transformation",
                          json=transform_request.dict(by_alias=True, exclude_none=True))
        return r.json()['request_id']

    async def get_transform_status(self, request_id: str):
        async with httpx.AsyncClient() as client:
            r = await client.get(url=f"{self.url}/servicex/transformation/{request_id}")
            status = TransformStatus(**r.json())
            return status
