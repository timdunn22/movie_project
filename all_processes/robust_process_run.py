class RobustProcessRun:

    def __init__(self, method, method_args, exception_tolerance):
        self.method = method
        self.exception_tolerance = exception_tolerance
        self.method_args = method_args
        self.exceptions = 0

    def run_process(self):
        while self.exceptions < self.exception_tolerance:
            try:
                self.method(**self.method_args)
            except:
                self.exceptions += 1
        

        