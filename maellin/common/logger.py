#   Copyright (C) 2022  Carl Chatterton. All Rights Reserved.
#
#   This program is free software: you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation, either version 3 of the License, or
#   (at your option) any later version.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with this program.  If not, see <https://www.gnu.org/licenses/>.

import sys
import logging


FORMAT = '%(asctime)s :: %(name)s :: %(levelname)s :: %(message)s'


class LoggingMixin(object):
    """
    Convenient Mixin to have a logger configured with the class name
    """
    @property
    def logger(self, level: str = 'INFO', **kwargs):
        logging.basicConfig(stream=sys.stdout, level=level, format=FORMAT, **kwargs)
        name = '.'.join([self.__class__.__module__, self.__class__.__name__])
        return logging.getLogger(name)
