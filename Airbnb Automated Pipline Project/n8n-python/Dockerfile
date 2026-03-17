FROM python:3.11-alpine AS pybuilder

RUN apk add --no-cache libstdc++ libgcc libffi
RUN pip install --no-cache-dir pandas numpy openpyxl pyarrow

FROM n8nio/n8n:latest

USER root

COPY --from=pybuilder /usr/local /usr/local
COPY --from=pybuilder /usr/lib/libstdc++.so.6 /usr/lib/libstdc++.so.6
COPY --from=pybuilder /usr/lib/libgcc_s.so.1 /usr/lib/libgcc_s.so.1
COPY --from=pybuilder /usr/lib/libffi.so.8 /usr/lib/libffi.so.8

RUN ln -sf /usr/local/bin/python3 /usr/local/bin/python && \
    ln -sf /usr/local/bin/pip3 /usr/local/bin/pip

USER node