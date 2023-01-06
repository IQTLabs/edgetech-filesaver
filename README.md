# edgetech-filesaver
EdgeTech filesaving utility for the `edgetech-core` program.

Subscribe to a specified topic and save data passed over it as a file. 

## Parameters
There are four key environment variables specified in `docker-compose.yml` the filesaver looks for. 
- `FILETOPIC` is a **string** value specifying which MQTT topic the filesaver should listen for data on. Following the topic format determined by the IQT Labs team, this should be in the form of `Data_Type/Container_Name/Type_Literal`. Default: `/data/filesave/binary`
- `FILESAVENAME` is a **string** value specifying the path of the file in which to save received data into. Default: `filevolume/datafile`
- `FILEAPPEND` is a **boolean** value indicating whether or not the utility should append to the save file or overwrite it every time new data is received. Default: `false`
- `FILETIMESTAMP` is a **boolean** value indicating whether a timestamp should be place at the end of the data's filename before saving. Default: `false`