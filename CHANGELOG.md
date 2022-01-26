# Change Log
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
