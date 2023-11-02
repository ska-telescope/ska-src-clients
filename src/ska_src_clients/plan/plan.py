from importlib import import_module
import json
import logging
import typing


class Plan:
    def __init__(self, **kwargs):
        self._current_step_number = 0
        self.steps = []

    @property
    def current_step_number(self):
        return self._current_step_number

    @current_step_number.setter
    def current_step_number(self, new_current_step_number):
        self._current_step_number = new_current_step_number

    @property
    def max_step_number(self):
        return self.number_of_steps-1

    @property
    def number_of_steps(self):
        return len(self.steps)

    @property
    def sections(self):
        sections = set()
        for step in self.steps:
            sections.add(step.section_name)
        return sections

    def append_step(self, section_name: str, fqn: str, arguments: typing.Dict[typing.Any, typing.Any] = {},
                    is_done: bool = False) -> None:
        """ Append a step to the plan.

        :param str section_name: section name to run next step from (will skip other sections in between)
        :param str fqn: the fully qualified function name (package and class)
        :param dict arguments: arguments to the function
        :param bool is_done: flag for whether step is done
        """
        self.steps.append(Step(section_name, fqn, arguments, is_done))

    def clear(self) -> None:
        """ Clear the current plan. """
        logging.debug("Clearing current plan")
        self.steps = []
        self.current_step_number = 0

    def describe(self) -> None:
        """ Describe the current plan. """
        print()
        print("Plan Description")
        print("================")
        print()
        for idx, step in enumerate(self.steps):
            section_name, fqn, arguments, is_done = (step.section_name, step.fqn, step.arguments, step.is_done)
            if not is_done:
                if hasattr(step.fqn, '__self__'): # bound method
                    print("{}: ({}) RUN bound method {}.{}.{} with parameters {}".format(
                        idx, section_name, fqn.__self__.__class__.__name__, fqn.__name__, fqn.__module__,
                        arguments))
                else:
                    print("{}: ({}) RUN function {}.{} with parameters {}".format(
                        idx, section_name, fqn.__module__, fqn.__name__, arguments))
            else:
                if hasattr(step.fqn, '__self__'):  # bound method
                    print("{}: ({}) RAN bound method {}.{}.{} with parameters {}".format(
                        idx, section_name, fqn.__self__.__class__.__name__, fqn.__name__, fqn.__module__,
                        arguments))
                else:
                    print("{}: ({}) RAN function {}.{} with parameters {}".format(
                        idx, section_name, fqn.__module__, fqn.__name__, arguments))
        print()

    @classmethod
    def load(cls, path: str) -> None:
        """ Load plan from a hard copy.

        :param str path: the path to load from
        """
        logging.info("Loading plan from file {}".format(path))
        with open(path, 'r') as fi:
            inputs = json.load(fi)
        plan = cls(**inputs)
        plan.current_step_number = inputs['current_step_number']
        function_classes_to_objects = {}        # avoid instantiating duplicate classes of same type
        for step in inputs['steps']:
            module = import_module(step['function_module_name'])
            if step['function_class_name']:     # bound method
                try:
                    if step['function_class_name'] not in function_classes_to_objects:
                        function_classes_to_objects[step['function_class_name']] = \
                        getattr(module, step['function_class_name'])()
                    function_class_instance = function_classes_to_objects[step['function_class_name']]
                    fqn = getattr(function_class_instance, step['function_name'])
                except AttributeError:          # bound method with no class, just module
                    fqn = getattr(module, step['function_name'])
            else:                               # unbound method (e.g. class)
                fqn = getattr(module, step['function_name'])
            plan.append_step(step['section_name'], fqn, arguments=step['arguments'], is_done=step['is_done'])
        return plan

    def run(self, section_name: str = None, dry_run: bool = False) -> typing.List[typing.Any]:
        """ Run the entire plan.

        :param str section_name: section name to run next step from (will skip other sections in between)
        :param bool dry_run: don't actually do anything, just print
        """
        logging.info("Running plan")
        returns = []
        while True:
            try:
                returns.append(self.run_next_step(section_name, dry_run))
                if self.current_step_number > self.max_step_number:
                    logging.info("Reached end of plan")
                    return
            except (Exception, KeyboardInterrupt) as e:
                logging.critical("Encountered exception running step {}: {}".format(self.current_step_number,
                                                                                    repr(e)))
                self.save("plan-dump.json")
                exit()
        return returns

    def run_next_step(self, section_name: str = None, dry_run: bool = False) -> typing.Any:
        """ Run the next step.

        :param str section_name: section name to run next step from (will skip other sections in between)
        :param bool dry_run: don't actually do anything, just print
        """
        if section_name:
            for idx, step in enumerate(self.steps[self.current_step_number:]):
                if step.section_name == section_name:
                    self.current_step_number += idx
                    break
        current_step = self.steps[self.current_step_number]

        section_name, fqn, arguments, is_done = \
            (current_step.section_name, current_step.fqn, current_step.arguments, current_step.is_done)
        if hasattr(fqn, '__self__'):  # bound method
            logging.debug("{}: ({}) Running function {}.{}.{} with parameters {}".format(
                self.current_step_number, section_name, fqn.__self__.__class__.__name__, fqn.__name__,
                fqn.__module__, arguments))
        else:
            logging.debug("{}: ({}) Running function {}.{} with parameters {}".format(
                self.current_step_number, section_name, fqn.__module__, fqn.__name__, arguments))
        if not dry_run:
            rtn = fqn(**arguments)
        else:
            rtn = None

        self.steps[self.current_step_number].is_done = True
        self.current_step_number += 1

        return rtn

    def save(self, path: str) -> None:
        """ Save a hard copy of the plan.

        :param path: the path to save to
        """
        logging.info("Saving plan to file {}".format(path))
        step_output = []
        for step in self.steps:
            section_name, fqn, arguments, is_done = (step.section_name, step.fqn, step.arguments, step.is_done)
            if hasattr(fqn, '__self__'):  # bound method
                step_output.append({
                    'section_name': section_name,
                    'function_name': fqn.__name__,
                    'function_class_name': fqn.__self__.__class__.__name__,
                    'function_module_name': fqn.__module__,
                    'arguments': arguments,
                    'is_done': is_done
                })
            else:
                step_output.append({
                    'section_name': section_name,
                    'function_name': fqn.__name__,
                    'function_class_name': None,
                    'function_module_name': fqn.__module__,
                    'arguments': arguments,
                    'is_done': is_done
                })
        output = {
            'current_step_number': self.current_step_number,
            'steps': step_output
        }
        with open(path, 'w') as fi:
            json.dump(output, fi, indent=2)


class UploadPlan(Plan):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
