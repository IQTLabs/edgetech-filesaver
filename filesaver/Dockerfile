FROM iqtlabs/edgetech-core:latest
COPY pyproject.toml .
RUN pip3 install poetry
RUN poetry config virtualenvs.create false
RUN poetry install --no-dev
ADD ./file_saver_pub_sub.py .
ENTRYPOINT [ "python3", "file_saver_pub_sub.py" ]