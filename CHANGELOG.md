# Change Log

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.4.5] - 2024-10-21

### Changed

- Upgraded requests to 2.32.0
- Upgraded urllib3 to 1.26.19
- Upgraded black to 24.3.0
- Onboarded anonymized operational metrics.

## [1.4.4] - 2023-10-13

### Changed

- Upgrade avro to 1.11.3

## [1.4.3] - 2023-10-12

### Changed

- Upgrade aws-cdk to 2.88.0
- Upgrade deprecated methods in App-registry
- Address or Fix all SonarQube issues

## [1.4.2] - 2023-06-22

### Changed

- Upgraded requests to 2.31.0 that addresses the [unintended leak of proxy-authorization header in requests](https://github.com/advisories/GHSA-j8r2-6x86-q33q)

## [1.4.1] - 2023-04-18

### Changed

- Updated the bucket policy on the logging bucket to grant access to the logging service principal (logging.s3.amazonaws.com) for access log delivery.
- Upgraded CDK version to 2.75.0

## [1.4.0] - 2023-03-29

### Changed

- Python library updates
- Upgraded Python runtime to 3.9
- Using `performAutoML` field in creating solutions now logs error, but proceeds to build the solution. This field is deprecated by the service.

### Added

- Github [issue #16](https://github.com/aws-solutions/maintaining-personalized-experiences-with-machine-learning/issues/16) `tags` are supported for all component types, for example, dataset group, import jobs, solutions, etc. Root-level tags are also supported in the config.
- "UPDATE" model training is supported for input solutions trained with the User-Personalization recipe or the HRNN-Coldstart recipe.

## [1.3.1] - 2022-12-19

### Fixed

- GitHub [issue #19](https://github.com/aws-solutions/maintaining-personalized-experiences-with-machine-learning/issues/19). This fix prevents AWS Service Catalog AppRegistry Application Name and Attribute Group Name from using a string that begins with `AWS`, since strings begining with `AWS` are considered as reserved words by the AWS Service.

### Changed

- Locked `boto3` to version `1.25.5`, and upgraded python library packages.

## [1.3.0] - 2022-11-17

### Added

[Service Catalog AppRegistry](https://docs.aws.amazon.com/servicecatalog/latest/arguide/intro-app-registry.html) resource to register the CloudFormation template and underlying resources as an application in both Service Catalog AppRegistry and AWS Systems Manager Application Manager

### Changed

- Upgraded CDK version to 2.44.0

## [1.2.0] - 2022-01-31

### Added

- The solution now supports batch segment jobs to get user segments with your solution version. Each user segment is
  sorted in descending order based on the probability that each user will interact with items in your inventory.
- The solution now supports domain dataset groups.

### Changed

- Upgraded to CDKv2.

## [1.1.0] - 2021-11-22

### Added

- The solution now creates an Amazon EventBridge event bus, and puts messages to the bus when resources have been
  created by the workflow. This can be useful when integrating with external systems.
- The solution now contains a command line interface (CLI) that allows schedule creation for existing resources in
  Amazon Personalize.

## [1.0.1] - 2021-10-01

### Added

- The solution now exports the Amazon SNS Topic ARN as `SNSTopicArn`.

### Changed

- The SNS message format will change based on the protocol used. For Amazon SQS and Email-JSON endpoints, a JSON payload
  will be sent. The message sent to subscribed Email endpoints is unchanged.
- The Amazon CloudWatch dashboard deployed by the solution will be replaced with a dashboard containing the stack's
  region name.

## [1.0.0] - 2021-09-23

### Added

- All files, initial version
