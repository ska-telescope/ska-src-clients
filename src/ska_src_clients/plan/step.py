class Step:
    def __init__(self, section_name, fqn, arguments, is_done=False):
        self._section_name = section_name
        self._fqn = fqn
        self._arguments = arguments
        self._is_done = is_done

    @property
    def section_name(self):
        return self._section_name

    @section_name.setter
    def section_name(self, new_section_name):
        self._section_name = new_section_name

    @property
    def fqn(self):
        return self._fqn

    @fqn.setter
    def fqn(self, new_fqn):
        self._fqn = new_fqn

    @property
    def arguments(self):
        return self._arguments

    @arguments.setter
    def arguments(self, new_arguments):
        self._arguments = new_arguments

    @property
    def is_done(self):
        return self._is_done

    @is_done.setter
    def is_done(self, new_is_done):
        self._is_done = new_is_done
