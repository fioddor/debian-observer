# SPDX-License-Identifier: AGPL-3.0-only
# Purpose: Container image for a python CLI application.
# Author.: Igor Zubiaurre <igor@zubiaurre.es>
#-----------------------------------------------------------------------------

FROM python:3.11-slim
WORKDIR /app
RUN apt-get update
RUN apt-get install -y pipx
ENV PATH="/root/.local/bin:$PATH"

