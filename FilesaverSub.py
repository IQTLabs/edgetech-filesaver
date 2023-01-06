from BaseMQTTPubSub import BaseMQTTPubSub
import os
from datetime import datetime
import schedule
from pathlib import Path

class FilesaverSub(BaseMQTTPubSub):
    def __init__(self) -> None:
        # Initialize BaseMQTTPubSub
        super().__init__(heartbeat_topic="/filesaver/heartbeat")

        # Connect to the broker and start the loop
        self.connect_client()

        # Define defaults for file saving variables
        self._FILETOPICDEFAULT = "/data/filesave/binary"
        self._FILESAVENAMEDEFAULT = "filevolume/datafile"
        self._FILEAPPENDDEFAULT = False
        self._FILETIMESTAMPDEFAULT = False

        # Get environment-set filesaving variables if they exist
        self.file_save_parameters = {
            "topic": os.environ["FILETOPIC"] if "FILETOPIC" in os.environ else self._FILETOPICDEFAULT,
            "name": os.environ["FILESAVENAME"] if "FILESAVENAME" in os.environ else self._FILESAVENAMEDEFAULT,
            "append": os.environ["FILEAPPEND"] if "FILEAPPEND" in os.environ else self._FILEAPPENDDEFAULT,
            "timestamp": os.environ["FILETIMESTAMP"] if "FILETIMESTAMP" in os.environ else self._FILETIMESTAMPDEFAULT
        }

        self.add_subscribe_topic(self.file_save_parameters["topic"], self._savefilecallback)

    def _savefile(self, filename: str, filedata: any, append: bool = False, timestamp: bool = False):
        # Create necessary directories if containing folders do not exist
        Path(os.path.dirname(filename)).mkdir(parents=True, exist_ok=True)

        # Determine the location of the file we're saving
        filelocation = filename + datetime.datetime.now().strftime("%d-%m-%Y-%X") if timestamp else filename

        # Append to the file if specified. Otherwise, overwrite it/write it fresh. 
        if append:
            with open(filename, 'ab') as filehandle:
                filehandle.write(filedata)
        else:
            with open(filename, 'wb') as filehandle:
                filehandle.write(filedata)

    def _savefilecallback(self, datadict: dict, msg: any):
        # Retrieve data to save.
        savedata = msg.payload.decode("utf-8")

        # Save that data.
        self._savefile(
            self.file_save_parameters["filename"], 
            savedata, 
            self.file_save_parameters["append"], 
        self.file_save_parameters["timestamp"])

    def heartbeat(self):
        self.publish_to_topic(self.HEARTBEAT_TOPIC, f'Filesaver Heartbeat')

    def main(self):
        # Send acknowledgement/heartbeat signal
        schedule.every(10).seconds.do(self.heartbeat)


if __name__ == "__main__":
    filesaver = FilesaverSub()
    filesaver.main()