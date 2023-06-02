FROM ghcr.io/binkhq/python:3.11-poetry AS build
WORKDIR /src
ADD . .

RUN poetry build && apt-get update

FROM ghcr.io/binkhq/python:3.11
WORKDIR /app
COPY --from=build /src/dist/*.whl .
RUN pip install *.whl && rm *.whl


ENTRYPOINT [ "linkerd-await", "--" ]
CMD [ "/usr/local/bin/sushictl", "upload" ]
