FROM iqtlabs/edgetech-core:latest

WORKDIR /root

ADD FilesaverSub.py .

ENTRYPOINT [ "python", "FilesaverSub.py" ]