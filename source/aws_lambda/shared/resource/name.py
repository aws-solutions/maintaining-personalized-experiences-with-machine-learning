# ######################################################################################################################
#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.                                                  #
#                                                                                                                      #
#  Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance      #
#  with the License. You may obtain a copy of the License at                                                           #
#                                                                                                                      #
#   http://www.apache.org/licenses/LICENSE-2.0                                                                         #
#                                                                                                                      #
#  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed    #
#  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for   #
#  the specific language governing permissions and limitations under the License.                                      #
# ######################################################################################################################


def camel_to_snake(s):
    """
    Convert a camelCasedName to a snake_cased_name
    :param s: the camelCasedName
    :return: the snake_cased_name
    """
    return "".join(["_" + c.lower() if c.isupper() else c for c in s]).lstrip("_")


def snake_to_camel(s: str):
    """
    Convert a snake_cased_name to a camelCasedName
    :param s: the snake_cased_name
    :return: camelCasedName
    """
    components = s.split("_")
    return components[0] + "".join(y.title() for y in components[1:])


def camel_to_dash(s: str):
    """
    Convert a camelCasedName to a dash-cased-name
    :param s: the camelCasedName
    :return: the dash-cased-name
    """
    return "".join(["-" + c.lower() if c.isupper() else c for c in s]).lstrip("-")


class ResourceName:
    def __init__(self, name: str):
        self.name = self._validated_name(name)

    def _validated_name(self, name) -> str:
        """
        Validate that a name is valid, raising ValueError if it is not
        :param name: the name to validate
        :return: the validated name
        """
        if not name.isalpha():
            raise ValueError("name must be camelCased")
        if not name[0].islower():
            raise ValueError("name must start with a lower case character")
        return name

    @property
    def dash(self) -> str:
        """
        Get the dash-cased-name of the resource
        :return: the dash-cased-name
        """
        return camel_to_dash(self.name)

    @property
    def snake(self) -> str:
        """
        Get the snake_cased_name of the resource
        :return: the snake_cased_name
        """
        return camel_to_snake(self.name)

    @property
    def camel(self) -> str:
        """
        Get the camelCasedName of the resource
        :return: the camelCasedName
        """
        return self.name
