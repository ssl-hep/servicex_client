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
from typing import Optional

import rich
import typer
from rich.table import Table

from servicex_client.models import Status
from servicex_client.servicex_adapter import ServiceXAdapter

transforms_app = typer.Typer(name="transforms", no_args_is_help=True)

@transforms_app.callback()
def transforms():
    """
    sub-commands for creating and manipulating Gardens
    """
    pass

@transforms_app.command(no_args_is_help=True)
def list(
        url: Optional[str] = typer.Option(
            None, "-u", "--url", help="URL of ServiceX server"
        ),
        complete: Optional[bool] = typer.Option(
            None, "--complete", help="Only show successfully completed transforms"
        )):
    sx = ServiceXAdapter(url)
    table = Table(title="ServiceX Transforms")
    table.add_column("Transform ID")
    table.add_column("Title")
    table.add_column("Status")
    for t in sx.get_transforms():
        if not complete or complete and t.status == Status.complete:
            table.add_row(t.request_id, "Not implemented", t.status)

    rich.print(table)

