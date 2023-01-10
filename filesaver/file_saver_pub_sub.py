import os
import json
from time import sleep
from datetime import datetime
from typing import Any, Dict

import schedule
import paho.mqtt.client as mqtt

from base_mqtt_pub_sub import BaseMQTTPubSub


class FileSaverPubSub(BaseMQTTPubSub):
    def __init__(
        self: Any,
        to_save_topic: str,
        c2c_topic: str,
        data_root: str,
        sensor_directory_name: str,
        file_prefix: str,
        **kwargs: Any
    ) -> None:
        super().__init__(**kwargs)

        self.to_save_topic = to_save_topic
        self.c2c_topic = c2c_topic
        self.save_path = os.path.join(data_root, sensor_directory_name)
        self.file_prefix = file_prefix
        self.file_timestamp = ""
        self.file_suffix = ".json"
        self.file_name = self.file_prefix + self.file_timestamp + self.file_suffix
        self.prev_file = None

        self.connect_client()
        sleep(1)
        self.publish_registration("File Saver Registration")

        os.makedirs(self.save_path, exist_ok=True)

        self._setup_new_write_file()

    def _setup_new_write_file(self: Any) -> None:
        self.file_timestamp = str(int(datetime.utcnow().timestamp()))
        self.file_name = self.file_prefix + self.file_timestamp + self.file_suffix
        self.file_path = os.path.join(self.save_path, self.file_name)

        with open(self.file_path, encoding="utf-8", mode="a") as file_pointer:
            file_pointer.write("[")

        if not self.prev_file:
            self.prev_file = self.file_path
        else:
            with open(self.prev_file, encoding="utf-8", mode="a") as file_pointer:
                file_pointer.write("\n]")
            self.prev_file = self.file_path

    def _to_save_callback(
        self: Any, _client: mqtt.Client, _userdata: Dict[Any, Any], msg: Any
    ) -> None:
        print("HERE")
        payload_json_str = str(msg.payload.decode("utf-8"))
        with open(self.file_path, encoding="utf-8", mode="a") as file_pointer:
            file_pointer.write("\n\t" + payload_json_str + ",")

    def _c2c_callback(
        self: Any, _client: mqtt.Client, _userdata: Dict[Any, Any], msg: Any
    ) -> None:
        c2c_payload = json.loads(str(msg.payload.decode("utf-8")))
        print(c2c_payload)
        if c2c_payload["msg"] == "NEW FILE":
            self._setup_new_write_file()

    def main(self: Any) -> None:
        schedule.every(10).seconds.do(
            self.publish_heartbeat, payload="File Saver Heartbeat"
        )

        self.add_subscribe_topics(
            [self.to_save_topic, self.c2c_topic],
            [self._to_save_callback, self._c2c_callback],
            [2, 2],
        )

        while True:
            schedule.run_pending()
            sleep(0.001)


if __name__ == "__main__":
    saver = FileSaverPubSub(
        to_save_topic=os.environ.get("TO_SAVE_TOPIC"),
        c2c_topic=os.environ.get("C2C_TOPIC"),
        data_root=os.environ.get("DATA_ROOT"),
        sensor_directory_name=os.environ.get("SENSOR_DIR"),
        file_prefix=os.environ.get("FILE_PREFIX"),
        mqtt_ip=os.environ.get("MQTT_IP")
    )
    saver.main()
