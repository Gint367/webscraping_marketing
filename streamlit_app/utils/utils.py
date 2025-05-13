import logging
import subprocess
from typing import Tuple

# It's good practice to get a logger instance specific to this module.
utils_logger = logging.getLogger(__name__)


def check_process_details_by_pid(pid: int) -> Tuple[bool, str]:
    """
    Checks if a process with the given PID is alive using bash commands
    and retrieves its details, including the command line.

    Args:
        pid: The process ID to check. Must be an integer.

    Returns:
        A tuple (is_alive: bool, details_string: str).
        is_alive is True if the process is running, False otherwise.
        details_string contains formatted process information if alive,
        or a message indicating it's not found or an error occurred.
    """
    if not isinstance(pid, int):
        utils_logger.error(f"Invalid PID type: {type(pid)}. PID must be an integer.")
        return False, "Error: PID must be an integer."

    is_alive = False
    details_string = f"Details for PID {pid} not found."

    try:
        # Step 1: Check aliveness using 'kill -0 PID'
        alive_check_command = ["kill", "-0", str(pid)]
        alive_check_result = subprocess.run(
            alive_check_command,
            capture_output=True,
            text=True,
            check=False,  # We handle the return code manually
        )

        if alive_check_result.returncode == 0:
            # Process is alive, try to get details
            try:
                # Step 2: Get details using 'ps'
                ps_command = [
                    "ps",
                    "-p",
                    str(pid),
                    "-o",
                    "pid=,ppid=,comm=,args=",
                    "--no-headers",
                ]
                ps_result = subprocess.run(
                    ps_command,
                    capture_output=True,
                    text=True,
                    check=True,  # Raise CalledProcessError if ps fails
                )
                output_line = ps_result.stdout.strip()

                if output_line:
                    parts = output_line.split(maxsplit=3)
                    if len(parts) == 4:
                        pid_val, ppid_val, comm_val, args_val = parts
                        details_string = (
                            f"PID: {pid_val.strip()} | PPID: {ppid_val.strip()} | "
                            f"Executable: '{comm_val.strip()}' | "
                            f"Full Command: '{args_val.strip()}'"
                        )
                        is_alive = True
                    else:
                        details_string = f"Process PID {pid} is alive, but details format is unexpected: '{output_line}'"
                        utils_logger.warning(
                            f"Unexpected ps output for PID {pid}: {output_line}"
                        )
                        is_alive = True
                else:
                    details_string = (
                        f"Process PID {pid} is alive, but ps returned no details."
                    )
                    utils_logger.warning(
                        f"ps command returned no output for alive PID {pid}."
                    )
                    is_alive = True

            except subprocess.CalledProcessError as e_ps:
                err_msg = e_ps.stderr.strip() if e_ps.stderr else str(e_ps)
                details_string = f"Process PID {pid} was alive but vanished or details could not be fetched. PS Error: {err_msg}"
                utils_logger.warning(
                    f"ps command failed for PID {pid} after alive check: {e_ps}"
                )
                is_alive = False
            except FileNotFoundError:
                details_string = (
                    "Error: 'ps' command not found. Please ensure it is in PATH."
                )
                utils_logger.error("'ps' command not found.")
                is_alive = False
        else:
            error_msg = (
                alive_check_result.stderr.strip()
                if alive_check_result.stderr
                else "process not found or no permission"
            )
            details_string = f"Process PID {pid} not found/alive or permission denied. (kill -0: {error_msg})"
            is_alive = False

    except FileNotFoundError:
        details_string = "Error: 'kill' command not found. Please ensure it is in PATH."
        utils_logger.error("'kill' command not found.")
        is_alive = False
    except Exception as e:
        details_string = (
            f"An unexpected error occurred while checking PID {pid}: {str(e)}"
        )
        utils_logger.exception(f"Unexpected error checking PID {pid}:")
        is_alive = False

    return is_alive, details_string
