# GHZ Custom

A repository for managing and making gRPC calls using the `ghz` tool.

## Table of Contents

- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
- [Starting the Database](#starting-the-database)
- [Making gRPC Calls](#making-grpc-calls)
  - [Publish Call](#publish-call)
  - [Subscribe Call](#subscribe-call)
- [Sample Config-files Structure](#sample-config-files-structure)
- [Metadata used to monitor latency](#metadata-used-to-monitor-latency)
- [ Pictorial representation of end to end latency calculation](#pictorial-representation-of-end-to-end-latency-calculation)
- [building ghz for arm64 architecture](#building-ghz-for-arm64-architecture)
- [building ghz-web for arm64 architecture](#building-ghz-web-for-arm64-architecture)
- [Commands used for testing on L4S testbed](#commands-used-for-testing-on-L4S-testbed)
- [copy report locally](#copy-report-locally)
- [graph generator](#graph-generator)



## Getting Started

### Prerequisites

Before you begin, ensure you have met the following requirements:

- You have `git` installed.
- You have `chmod` and `sudo` permissions.

### Installation

To clone the repository, run:

```sh
git clone https://github.boschdevcloud.com/SAI1RNG/ghz-custom.git -b main
```

## Starting the Database

```sh
cd ghz-custom/cmd/ghz-web
chmod +x ./ghz-web
sudo ./ghz-web
```
## Making gRPC Calls
```sh
cd ghz-custom/cmd/ghz
chmod +x ./ghz
```
### Publish Call
```sh
./ghz --insecure --config=config.json
```

### Subscribe Call
```sh
./ghz --insecure --config=config_subscribe.json json
```

### Publish Call (with Database)
```sh
./ghz --insecure --config=config.json -O json | http POST localhost:80/api/ingest
```

### Subscribe Call (with Database)
```sh
./ghz --insecure --config=config_subscribe.json -O json | http POST localhost:80/api/ingest
```

## Sample Config-files Structure
<div>
  <div style="float: left; width: 45%;">
    <ol>
      <li><strong>timeout</strong>: The amount of time the ghz is required to subscribe to kuksa-databroker <code>{{.RequestNumber}}</code> placeholder.</li>
      <li><strong>request_id</strong>: This acts as a identifier to differntiate between multiple tests conducted. Example: <code>50sub300pub</code> indicates an experiment with 50 subscribers and 300 publish calls, request_id can be anything helping identify between tests.</li>
      <li><strong>description</strong>: Provides additional information about the request. Uses the <code>{{.RequestNumber}}</code> placeholder.</li>
      <li><strong>fields</strong>: An array specifying data fields to be kept track of, such as <code>["2", "12"]</code>, here having <b>"12"</b> is a must.</li>
    </ol>
  </div>
  <div style="float: right; width: 45%;">
    <img src="./images/set_call.jpg" alt="Publish Config" width="100%">
    <br>
    <img src="./images/subscribe_call.jpg" alt="Subscribe Config" width="100%">
  </div>
  <div style="clear: both;"></div>
</div>


Note: The combination of `request_id` and `description` forms a unique id for identifying a particular publish call in a given test.

## Metadata used to monitor latency

<img width="1466" alt="kuksa_set_subscribe_postman" src="./images/postman_image.png">

- latency related metadata for publish call is recieved in the metadata of Http respone.
- latency related metadata for subscribe call is received within the payload of the subscription response.

## Pictorial representation of end to end latency calculation

<img width="1157" alt="metadata_capture" src="./images/pictorial_representation.png">

## Report generation

```sh
cd cmd/ghz-web

# open in vscode in devcontainer

python sqlite-latency-extractor-dev.py

# This generates a file named latency_and_mean_stats.xlsx 
```


