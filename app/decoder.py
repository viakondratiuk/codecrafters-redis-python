from app import constants

class RESPDecoder:
    @staticmethod
    def decode(data: bytes):
        commands = []
        i = 0
        n = len(data)

        while i < n:
            if data[i : i + 1] == b"*":
                # Start position of the command
                command_start = i
                
                # Read the number of elements in the array
                end_of_line = data.index(constants.BTERM, i)
                num_elements = int(data[i + 1 : end_of_line].decode())
                i = end_of_line + 2

                # Read the specified number of elements
                command = []
                for _ in range(num_elements):
                    if data[i : i + 1] == b"$":
                        # Read the length of the next string
                        end_of_line = data.index(constants.BTERM, i)
                        str_length = int(data[i + 1 : end_of_line].decode())
                        i = end_of_line + 2

                        # Read the string of the specified length
                        argument = data[i : i + str_length].decode()
                        command.append(argument)
                        i += str_length + 2  # Move to the end of the string and skip \r\n

                # End position of the command
                command_end = i

                # Calculate the command length in bytes
                command_length = command_end - command_start

                # Insert the length as the second element in the command list
                commands.append([command[0], command_length] + command[1:])

        return commands
