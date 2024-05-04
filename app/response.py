class Response:
    @staticmethod
    def _format(payload: list):
        return "\r\n".join(payload + [""])

    @staticmethod
    def _build_response(prefix: str, messages: tuple, formatter=lambda x: x):
        payload = [f"{prefix}{formatter(message)}" for message in messages]
        return Response._format(payload)

    @staticmethod
    def ok(*messages):
        return Response._build_response("+", messages)

    @staticmethod
    def error(*messages):
        return Response._build_response("-", messages)

    @staticmethod
    def data(*messages):
        return Response._build_response(
            "$", messages, lambda msg: f"{len(msg)}\r\n{msg}"
        )

    @staticmethod
    def null(*messages):
        return Response._format(["$-1"])
