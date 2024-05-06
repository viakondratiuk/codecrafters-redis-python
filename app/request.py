class Request:
    @staticmethod
    def parse(request: str):
        args = [arg.lower() for arg in request.strip().split("\r\n")]
        return args[2], [
            arg for idx, arg in enumerate(args) if idx >= 4 and idx % 2 == 0
        ]
