# first stage
FROM python:3.9 AS builder
COPY requirements.txt .

# install dependencies to the local user directory (eg. /root/.local)
RUN pip install --user -r requirements.txt

# second unnamed stage
FROM python:3.9-slim
WORKDIR /code

# copy only the dependencies installation from the 1st stage image
COPY --from=builder /root/.local /root/.local
COPY rss2telegram.py .

# update PATH environment variable
ENV PATH=/root/.local:$PATH

CMD [ "python", "./rss2telegram.py" ] 
