FROM python:3.12 as builder

WORKDIR /home

RUN pip install -q --upgrade --upgrade-strategy eager pip setuptools wheel && \
    pip install pdm

COPY pyproject.toml pdm.lock .

RUN pdm sync -G :all --prod --no-editable --no-self

COPY . .

RUN pdm sync -G :all --prod --no-editable

# Run Stage
FROM python:3.12

WORKDIR /home

ENV VIRTUAL_ENV /home/.venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
COPY --from=builder $VIRTUAL_ENV $VIRTUAL_ENV

ENTRYPOINT ["python", "-m", "awslambdaric"]
CMD ["economic_data.lambda_handlers.fetch_series_handler"]
