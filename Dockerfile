FROM astrocrpublic.azurecr.io/runtime:3.3-7

# FROM quay.io/astronomer/astro-runtime:3.2-1

# USER root

# RUN apt-get update && \
#     apt-get install -y --no-install-recommends \
#         unixodbc \
#         unixodbc-dev \
#         libltdl7 && \
#     rm -rf /var/lib/apt/lists/*

# USER astro

# RUN pip install --no-cache-dir pyodbc
