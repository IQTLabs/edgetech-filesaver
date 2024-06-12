"""This file contains the FileSaverPubSub class which is the child class of BaseMQTTPubSub.
The FileSaverPubSub writes data published to topics of interest via MQTT and writes them to a file.
The C2 topic is used to trigger the writing of additional files at an interval specified.
"""
import os
import json
from time import sleep
from datetime import datetime
from typing import Any, Dict, Union

import schedule
import paho.mqtt.client as mqtt

from base_mqtt_pub_sub import BaseMQTTPubSub


class FileSaverPubSub(BaseMQTTPubSub):
    """This class writes data payloads published over MQTT topics to JSON[s].

    Args:
        BaseMQTTPubSub (BaseMQTTPubSub): parent class written in the EdgeTech Core module.
    """

    def __init__(
        self: Any,
        sensor_save_topic: str,
        telemetry_save_topic: str,
        audio_base64_save_topic: str,
        imagery_base64_save_topic: str,
        c2c_topic: str,
        data_root: str,
        sensor_directory_name: str,
        telemetry_directory_name: str,
        base64_audio_directory_name: str,
        base64_imagery_directory_name: str,
        sensor_file_prefix: str,
        telemetry_file_prefix: str,
        base64_audio_file_prefix: str,
        base64_imagery_file_prefix: str,
        debug: bool = False,
        **kwargs: Any
    ) -> None:
        """The FileSaverPubSub takes topics to subscribe to and directories to write to in order
        to orchestrate the saving of data at regular intervals.

        Args:
            sensor_save_topic (str): topic to subscribe to and read sensor data from.
            telemetry_save_topic (str): topic to subscribe to and read telemetry data from.
            c2c_topic (str): command and control topic to trigger writing to a new file for
            clock consistency.
            data_root (str): the path to the root directory where data is saved, this is a
            docker volume.
            sensor_directory_name (str): the name of the sensor directory, based on kind of data.
            telemetry_directory_name (str): the name of the telemetry directory.
            sensor_file_prefix (str): the file prefix for the sensor data, based on kind of data.
            telemetry_file_prefix (str): the file prefix for the telemetry data.
            debug (bool, optional): If the debug mode is turned on, log statements print to stdout.
            Defaults to False.
        """
        # pass kwargs to super class to override class variables
        super().__init__(**kwargs)

        # saving topics
        self.sensor_save_topic = sensor_save_topic
        self.telemetry_save_topic = telemetry_save_topic
        self.audio_base64_save_topic = audio_base64_save_topic
        self.imagery_base64_save_topic = imagery_base64_save_topic

        # trigger topic
        self.c2c_topic = c2c_topic

        # toggle logging
        self.debug = debug

        # file write directories
        self.sensor_save_path = os.path.join(data_root, sensor_directory_name)
        self.telemetry_save_path = os.path.join(data_root, telemetry_directory_name)
        self.base64_audio_save_path = os.path.join(
            data_root, base64_audio_directory_name
        )
        self.base64_imagery_save_path = os.path.join(
            data_root, base64_imagery_directory_name
        )

        # file prefix by data
        self.sensor_file_prefix = sensor_file_prefix
        self.telemetry_file_prefix = telemetry_file_prefix
        self.base64_audio_file_prefix = base64_audio_file_prefix
        self.base64_imagery_file_prefix = base64_imagery_file_prefix

        # files saved with write timestamps
        self.sensor_file_timestamp = ""
        self.telemetry_file_timestamp = ""
        self.base64_audio_file_timestamp = ""
        self.base64_imagery_file_timestamp = ""

        # this module writes JSON[s]
        self.file_suffix = ".json"

        # composing filenames
        self.sensor_file_name = (
            self.sensor_file_prefix + self.sensor_file_timestamp + self.file_suffix
        )
        self.telemetry_file_name = (
            self.telemetry_file_prefix
            + self.telemetry_file_timestamp
            + self.file_suffix
        )
        self.base64_audio_file_name = (
            self.base64_audio_file_prefix
            + self.base64_audio_file_timestamp
            + self.file_suffix
        )
        self.base64_imagery_file_name = (
            self.base64_imagery_file_prefix
            + self.base64_imagery_file_timestamp
            + self.file_suffix
        )

        # connecting to MQTT server
        self.connect_client()
        sleep(1)
        self.publish_registration("File Saver Registration")

        # creating save directories if they do not exist
        os.makedirs(self.sensor_save_path, exist_ok=True)
        os.makedirs(self.telemetry_save_path, exist_ok=True)
        os.makedirs(self.base64_audio_save_path, exist_ok=True)
        os.makedirs(self.base64_imagery_save_path, exist_ok=True)

        # to initialize full path by combining save path and filename
        self.sensor_file_path = None
        self.telemetry_file_path = None
        self.base64_audio_file_path = None
        self.base64_imagery_file_path = None

        # create write files (JSON array initialization)
        self.sensor_file_path = self._setup_new_write_file(
            self.sensor_file_prefix,
            self.sensor_save_path,
            self.sensor_file_path,
        )
        self.telemetry_file_path = self._setup_new_write_file(
            self.telemetry_file_prefix,
            self.telemetry_save_path,
            self.telemetry_file_path,
        )
        self.base64_audio_file_path = self._setup_new_write_file(
            self.base64_audio_file_prefix,
            self.base64_audio_save_path,
            self.base64_audio_file_path,
        )
        self.base64_imagery_file_path = self._setup_new_write_file(
            self.base64_imagery_file_prefix,
            self.base64_imagery_save_path,
            self.base64_imagery_file_path,
        )

    def _setup_new_write_file(
        self: Any, file_prefix: str, save_path: str, file_path: Union[str, None]
    ) -> str:
        """If a previous file exists, then the JSON array is closed. Then, a new file is opened
        up for writing and initialized for writing.

        Args:
            file_prefix (str): the file prefix based on the kind of data.
            save_path (str): the absolute path to the directory to save to.
            file_path (Union[str, None]): the previous file if it exists else None.

        Returns:
            str: returns the new write file path
        """

        # if a previous file path exists, close the JSON array
        if file_path:
            with open(file_path, encoding="utf-8", mode="a") as file_pointer:
                file_pointer.write("\n]")

        # create new file path
        file_timestamp = str(datetime.utcnow().timestamp())
        file_name = file_prefix + file_timestamp + self.file_suffix
        file_path = os.path.join(save_path, file_name)

        # open new JSON array for writing
        with open(file_path, encoding="utf-8", mode="a") as file_pointer:
            file_pointer.write("[")

        # return the new file path for writing
        return file_path

    def _sensor_save_callback(
        self: Any, _client: mqtt.Client, _userdata: Dict[Any, Any], msg: Any
    ) -> None:
        """Callback for the sensor data that specifies writing the message payload to the
        sensor file.

        Args:
            _client (mqtt.Client): the MQTT client that was instantiated in the constructor.
            _userdata (Dict[Any,Any]): data passed to the callback through the MQTT paho Client
            class constructor or set later through user_data_set().
            msg (Any): the received message over the subscribed channel that includes
            the topic name and payload after decoding. The messages here will include the
            sensor data to save.
        """
        # decode the JSON payload from callback message
        payload_json_str = str(msg.payload.decode("utf-8"))

        # open and write to sensor file
        with open(self.sensor_file_path, encoding="utf-8", mode="a") as file_pointer:
            file_pointer.write("\n\t" + payload_json_str + ",")

    def _telemetry_save_callback(
        self: Any, _client: mqtt.Client, _userdata: Dict[Any, Any], msg: Any
    ) -> None:
        """Callback for the telemetry data that specifies writing the message payload to the
        telemetry file.

        Args:
            _client (mqtt.Client): the MQTT client that was instantiated in the constructor.
            _userdata (Dict[Any,Any]): data passed to the callback through the MQTT paho Client
            class constructor or set later through user_data_set().
            msg (Any): the received message over the subscribed channel that includes
            the topic name and payload after decoding. The messages here will include the
            telemetry data to save.
        """
        # decode the JSON payload from callback message
        payload_json_str = str(msg.payload.decode("utf-8"))

        # open and write to telemetry file
        with open(self.telemetry_file_path, encoding="utf-8", mode="a") as file_pointer:
            file_pointer.write("\n\t" + payload_json_str + ",")

    def _base64_audio_save_callback(
        self: Any, _client: mqtt.Client, _userdata: Dict[Any, Any], msg: Any
    ) -> None:
        """Callback for the telemetry data that specifies writing the message payload to the
        telemetry file.

        Args:
            _client (mqtt.Client): the MQTT client that was instantiated in the constructor.
            _userdata (Dict[Any,Any]): data passed to the callback through the MQTT paho Client
            class constructor or set later through user_data_set().
            msg (Any): the received message over the subscribed channel that includes
            the topic name and payload after decoding. The messages here will include the
            telemetry data to save.
        """
        # decode the JSON payload from callback message
        payload_json_str = str(msg.payload.decode("utf-8"))

        # open and write to telemetry file
        with open(
            self.base64_audio_file_path, encoding="utf-8", mode="a"
        ) as file_pointer:
            file_pointer.write("\n\t" + payload_json_str + ",")

    def _base64_imagery_save_callback(
        self: Any, _client: mqtt.Client, _userdata: Dict[Any, Any], msg: Any
    ) -> None:
        """Callback for the telemetry data that specifies writing the message payload to the
        telemetry file.

        Args:
            _client (mqtt.Client): the MQTT client that was instantiated in the constructor.
            _userdata (Dict[Any,Any]): data passed to the callback through the MQTT paho Client
            class constructor or set later through user_data_set().
            msg (Any): the received message over the subscribed channel that includes
            the topic name and payload after decoding. The messages here will include the
            telemetry data to save.
        """
        # decode the JSON payload from callback message
        payload_json_str = str(msg.payload.decode("utf-8"))

        # open and write to telemetry file
        with open(
            self.base64_imagery_file_path, encoding="utf-8", mode="a"
        ) as file_pointer:
            file_pointer.write("\n\t" + payload_json_str + ",")

    def _c2c_callback(
        self: Any, _client: mqtt.Client, _userdata: Dict[Any, Any], msg: Any
    ) -> None:
        """Callback for the C2 topic which currently triggers the changing of files to write to.

        Args:
            _client (mqtt.Client): the MQTT client that was instantiated in the constructor.
            _userdata (Dict[Any,Any]): data passed to the callback through the MQTT paho Client
            class constructor or set later through user_data_set().
            msg (Any): the received message over the subscribed channel that includes
            the topic name and payload after decoding. The messages here will include the C2
            triggers to switch files.
        """
        # decode the JSON payload from callback message
        c2c_payload = json.loads(str(msg.payload.decode("utf-8")))

        # if the payload is NEW FILE then setup new files to write to
        if c2c_payload["msg"] == "NEW FILE":
            self.sensor_file_path = self._setup_new_write_file(
                self.sensor_file_prefix,
                self.sensor_save_path,
                self.sensor_file_path,
            )
            self.telemetry_file_path = self._setup_new_write_file(
                self.telemetry_file_prefix,
                self.telemetry_save_path,
                self.telemetry_file_path,
            )
            self.base64_audio_file_path = self._setup_new_write_file(
                self.base64_audio_file_prefix,
                self.base64_audio_save_path,
                self.base64_audio_file_path,
            )
            self.base64_imagery_file_path = self._setup_new_write_file(
                self.base64_imagery_file_prefix,
                self.base64_imagery_save_path,
                self.base64_imagery_file_path,
            )

    def main(self: Any) -> None:
        """Main loop and function that setup the heartbeat to keep the TCP/IP
        connection alive and publishes the data to the MQTT broker and keeps the
        main thread alive."""

        # publish heartbeat to keep TCP/IP connection alive
        schedule.every(10).seconds.do(
            self.publish_heartbeat, payload="File Saver Heartbeat"
        )

        # subscribe to relevant topics
        self.add_subscribe_topics(
            [
                self.sensor_save_topic,
                self.telemetry_save_topic,
                self.audio_base64_save_topic,
                self.imagery_base64_save_topic,
                self.c2c_topic,
            ],
            [
                self._sensor_save_callback,
                self._telemetry_save_callback,
                self._base64_audio_save_callback,
                self._base64_imagery_save_callback,
                self._c2c_callback,
            ],
            [2, 2, 2, 2, 2],
        )

        # keep main thread alive
        while True:
            try:
                # flush pending scheduled tasks
                schedule.run_pending()
                # keep from running at CPU time
                sleep(0.001)
            except KeyboardInterrupt as exception:
                if self.debug:
                    print(exception)

                # close JSON arrays on interrupt
                if self.sensor_file_path:
                    with open(
                        self.sensor_file_path, encoding="utf-8", mode="a"
                    ) as file_pointer:
                        file_pointer.write("\n]")

                if self.telemetry_file_path:
                    with open(
                        self.telemetry_file_path, encoding="utf-8", mode="a"
                    ) as file_pointer:
                        file_pointer.write("\n]")
                if self.base64_audio_file_path:
                    with open(
                        self.base64_audio_file_path, encoding="utf-8", mode="a"
                    ) as file_pointer:
                        file_pointer.write("\n]")

                if self.base64_imagery_file_path:
                    with open(
                        self.base64_imagery_file_path, encoding="utf-8", mode="a"
                    ) as file_pointer:
                        file_pointer.write("\n]")


# interactive session
if __name__ == "__main__":
    # instantiate class object w/ env variables pass via docker-compose
    saver = FileSaverPubSub(
        sensor_save_topic=str(os.environ.get("SENSOR_TOPIC")),
        telemetry_save_topic=str(os.environ.get("TELEMETRY_TOPIC")),
        audio_base64_save_topic=str(os.environ.get("AUDIO_BASE64_TOPIC")),
        imagery_base64_save_topic=str(os.environ.get("IMAGE_BASE64_TOPIC")),
        c2c_topic=str(os.environ.get("C2_TOPIC")),
        data_root=str(os.environ.get("DATA_ROOT")),
        sensor_directory_name=str(os.environ.get("SENSOR_DIR")),
        telemetry_directory_name=str(os.environ.get("TELEMETRY_DIR")),
        base64_audio_directory_name=str(os.environ.get("BASE64_AUDIO_DIR")),
        base64_imagery_directory_name=str(os.environ.get("BASE64_IMAGERY_DIR")),
        sensor_file_prefix=str(os.environ.get("SENSOR_FILE_PREFIX")),
        telemetry_file_prefix=str(os.environ.get("TELEMETRY_FILE_PREFIX")),
        base64_audio_file_prefix=str(os.environ.get("BASE64_AUDIO_FILE_PREFIX")),
        base64_imagery_file_prefix=str(os.environ.get("BASE64_AUDIO_FILE_PREFIX")),
        mqtt_ip=str(os.environ.get("MQTT_IP")),
    )
    # spin
    saver.main()
