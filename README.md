

<a name="readme-top"></a>

[contributors-shield]: https://img.shields.io/github/contributors/IQTLabs/edgetech-filesaver.svg?style=for-the-badge
[contributors-url]: https://github.com/IQTLabs/edgetech-filesaver/graphs/contributors
[forks-shield]: https://img.shields.io/github/forks/IQTLabs/edgetech-filesaver.svg?style=for-the-badge
[forks-url]: https://github.com/IQTLabs/edgetech-filesaver/network/members
[stars-shield]: https://img.shields.io/github/stars/IQTLabs/edgetech-filesaver.svg?style=for-the-badge
[stars-url]: https://github.com/IQTLabs/edgetech-filesaver/stargazers
[issues-shield]: https://img.shields.io/github/issues/IQTLabs/edgetech-filesaver.svg?style=for-the-badge
[issues-url]: https://github.com/IQTLabs/edgetech-filesaver/issues
[license-shield]: https://img.shields.io/github/license/IQTLabs/edgetech-filesaver.svg?style=for-the-badge
[license-url]: https://github.com/IQTLabs/edgetech-filesaver/blob/master/LICENSE.txt
[product-screenshot]: images/screenshot.png

[Python]: https://img.shields.io/badge/python-000000?style=for-the-badge&logo=python
[Python-url]: https://www.python.org
[Poetry]: https://img.shields.io/badge/poetry-20232A?style=for-the-badge&logo=poetry
[Poetry-url]: https://python-poetry.org
[Docker]: https://img.shields.io/badge/docker-35495E?style=for-the-badge&logo=docker
[Docker-url]: https://www.docker.com

<br />
<div align="center">
  <a href="https://iqtlabs.org/">
    <img src="images/logo.png" alt="Logo" width="331" height="153">
  </a>

<h1 align="center">EdgeTech-Filesaver</h1>

  <p align="center">
    This repo builds upon the <a href="https://github.com/IQTLabs/edgetech-core">IQT Labs Edgetech-Core</a> functionality to instantiate an <a href="https://projects.eclipse.org/projects/iot.mosquitto">MQTT</a> client that subscribes to sensor and telemetry topics to write them to a file as JSON arrays. Functionalty is also included to respond to a <a href="https://github.com/IQTLabs/edgetech-c2">Command and Control</a> module to cycle to the next file. All of this functionality is wrapped in a Docker container for cross-platform compatability. 
    <br/>
    <br/>
    <a href="https://github.com/IQTLabs/edgetech-filesaver/pulls">Make Contribution</a>
    ·
    <a href="https://github.com/IQTLabs/edgetech-filesaver/issues">Report Bug</a>
    ·
    <a href="https://github.com/IQTLabs/edgetech-filesaver/issues">Request Feature</a>
  </p>
</div>

[![Contributors][contributors-shield]][contributors-url]
[![Forks][forks-shield]][forks-url]
[![Stargazers][stars-shield]][stars-url]
[![Issues][issues-shield]][issues-url]
[![MIT License][license-shield]][license-url]

### Built With

[![Python][Python]][Python-url]
[![Poetry][Poetry]][Poetry-url]
[![Docker][Docker]][Docker-url]

## Getting Started

To run this repo, simply run:

```
docker-compose up
```

The filesaver is containerized and dependencies are managed using [poetry]("https://python-poetry.org"). 

### Prerequisites

Running this repo requires that you have [Docker](https://www.docker.com) installed. 

## Usage

Spinning up this system requires an MQTT server and this container to be included in your `docker-compose.yml`. You can find an example of this workflow in this repository's `docker-compose.yml`. Additionally, some editing of relevant enviornment variables will be required based upon your system's configuration of topics to subscribe to and MQTT configuration. Examples of these enviornment variables can be found in this repository's `.env` file. 

As this system is meant to be spun up with MQTT topics you would like to write to files, copying the filesaver `docker-compose` statements into a master `docker-compose.yml` and  `.env` files with your entire system of containers is the preferred workflow. Find an application architecture diagram example of how the usage of this module was envisioned below.

<img src="images/diagram.png" alt="Diagram">

## Roadmap

- TBA

See the [open issues](https://github.com/github_username/repo_name/issues) for a full list of proposed features (and known issues).

## Contributing

1. Fork the Project
2. Create your Feature Branch (`git checkout -b dev`)
3. Commit your Changes (`git commit -m 'adding some feature'`)
4. Run (and make sure they pass):
```
black --diff --check *.py

pylint --disable=all --enable=unused-import *.py

mypy --allow-untyped-decorators --ignore-missing-imports --no-warn-return-any --strict *.py
```
If you do not have them installed, you can install them with `pip install "black<23" pylint==v3.0.0a3 mypy==v0.991`.

5. Push to the Branch (`git push origin dev`)
6. Open a Pull Request

See `CONTRIBUTING.md` for more information.

## License

Distributed under the [Apache 2.0](https://github.com/IQTLabs/edgetech-filesaver/blob/main/LICENSE). See `LICENSE.txt` for more information.

## Contact IQTLabs

  - Twtiter: [@iqtlabs](https://twitter.com/iqtlabs)
  - Email: info@iqtlabs.org

See our other projects: [https://github.com/IQTLabs/](https://github.com/IQTLabs/)

<p align="right">(<a href="#readme-top">back to top</a>)</p>




