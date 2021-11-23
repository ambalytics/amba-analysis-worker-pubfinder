FROM python:3.6

WORKDIR /src
COPY . .

RUN pip install --upgrade pip
RUN pip install --upgrade sentry-sdk
RUN pip install --pre gql[all]

COPY src/requirements.txt /requirements.txt
RUN pip install -r src/requirements.txt

ENTRYPOINT ["sh", "./scripts/entrypoint.sh"]