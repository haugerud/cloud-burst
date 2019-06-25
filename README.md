# cloud-burst

Simple proof-of-concept Python2 code for automated cloud bursting using Amazon Web Services and Apache Mesos.

## Setup
The depdendencies for this project are listed in `requirements.txt`. You can use `pip` to install the requirements.

```bash
pip install -r requirements.txt
```

## Configurations
There are two YAML files used for configuration:
* config.yml
* launch_config.yml

### config.yml
This configuration file contains the most of the configuration for the scripts.
You need to provide an AWS access key and a secret access key. The rest of the settings are defined with sane (at the time of creation) defaults that _should_ work.

### launch_config.yml
This contains the "Launch Configuration" used by Amazon Web Services for Auto Scaling.
Consult their [documentation](https://docs.aws.amazon.com/autoscaling/ec2/userguide/LaunchConfiguration.html) on this for more details.
The included example configuration contains `UserData` with the bash code used for bootstrapping the instances and join them with the cluster.
