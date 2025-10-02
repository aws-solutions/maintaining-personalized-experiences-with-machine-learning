# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

class ResourcePending(Exception):
    pass


class SolutionVersionPending(Exception):
    pass


class ResourceFailed(Exception):
    pass


class ResourceInvalid(Exception):
    pass


class ResourceNeedsUpdate(Exception):
    pass


class NotificationError(Exception):
    pass
