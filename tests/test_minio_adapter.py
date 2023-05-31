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
from unittest.mock import patch

import httpx
import pytest

from servicex_client.minio_adpater import MinioAdapter
from servicex_client.models import TransformStatus

test_transform = {'request_id': 'b8c508d0-ccf2-4deb-a1f7-65c839eebabf',
                  'did': 'File List Provided in Request', 'columns': None,
                  'selection': "(Where (SelectMany (call EventDataset) (lambda (list e) (call (attr e 'Jets') 'AntiKt4EMTopoJets'))) (lambda (list j) (and (> (/ (call (attr j 'pt')) 1000) 20) (< (call abs (/ (call (attr j 'eta')) 1000)) 4.5))))",
                  'tree-name': None,
                  'image': 'sslhep/servicex_func_adl_uproot_transformer:uproot4',
                  'workers': None, 'result-destination': 'object-store',
                  'result-format': 'parquet',
                  'workflow-name': 'selection_codegen',
                  'generated-code-cm': 'b8c508d0-ccf2-4deb-a1f7-65c839eebabf-generated-source',
                  'status': 'Submitted', 'failure-info': None,
                  'app-version': 'develop',
                  'code-gen-image': 'sslhep/servicex_code_gen_func_adl_uproot:v1.2.0',
                  'files': 1, 'files-completed': 0, 'files-failed': 0,
                  'files-remaining': 1,
                  'submit-time': '2023-05-25T20:05:05.564137Z',
                  'finish-time': 'None',
                  "minio-endpoint": 'minio.org:9000',
                  "minio-secured": True,
                  "minio-access-key": "miniouser",
                  "minio-secret-key": "secret"}


@pytest.mark.asyncio
async def test_initialize_from_status():
    transform = TransformStatus(**test_transform)
    minio = MinioAdapter.for_transform(transform)
    assert minio.minio._base_url.host == "minio.org:9000"
    assert minio.minio._provider._credentials.access_key == "miniouser"
    assert minio.minio._provider._credentials.secret_key == "secret"
    assert minio.bucket == "b8c508d0-ccf2-4deb-a1f7-65c839eebabf"

