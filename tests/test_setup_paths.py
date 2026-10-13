"""Setup keeps validated answers while recovering from path input failures."""

import io
import os
import sys
import tempfile
from pathlib import Path
from unittest import TestCase, skipUnless
from unittest.mock import patch

from hypothesis import example, given
from hypothesis import strategies as st

from ambientweather2sqlite import configuration
from ambientweather2sqlite.cli.commands import main
from ambientweather2sqlite.configuration import load_config

_URL = "http://station.local/livedata.htm"


class TestDatabasePaths(TestCase):
    def test_nul_input_reprompts_before_output_is_opened(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "config.toml"
            target.write_bytes(b"original")
            answers = iter(["n", _URL, "bad\x00.db", "weather.db", ""])

            def prompt(_message: str) -> str:
                self.assertEqual(target.read_bytes(), b"original")
                return next(answers)

            with (
                patch("builtins.input", side_effect=prompt) as inputs,
                patch("builtins.print") as output,
            ):
                created = configuration.create_config_file(
                    target, overwrite_existing=True
                )
            self.assertEqual(inputs.call_count, 5)
            self.assertEqual(load_config(created).database_path, "weather.db")
            self.assertIn("Invalid database path: NUL", output.call_args.args[0])

    def test_configured_nul_path_fails_before_database_and_network_activity(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "config.toml"
            target.write_text(
                f'live_data_url = "{_URL}"\ndatabase_path = "weather\\u0000.db"\n',
                encoding="utf-8",
            )
            with (
                patch(
                    "ambientweather2sqlite.cli.commands.create_database_if_not_exists"
                ) as database,
                patch("ambientweather2sqlite.cli.commands.start_daemon") as daemon,
                patch("ambientweather2sqlite.station_http._receiver_lock") as locks,
                patch("socket.getaddrinfo") as dns,
                patch("ambientweather2sqlite.mureq.get") as get,
                patch("ambientweather2sqlite.mureq.post") as post,
                patch("sys.stderr", new_callable=io.StringIO) as errors,
                self.assertRaises(SystemExit) as raised,
            ):
                main(["serve", "--config", str(target)])
            self.assertEqual(raised.exception.code, 1)
            self.assertIn("database_path is invalid: NUL", errors.getvalue())
            for activity in (database, daemon, locks, dns, get, post):
                activity.assert_not_called()


class TestOutputPaths(TestCase):
    @skipUnless(
        os.name == "posix", "POSIX rejects high surrogates in filesystem encoding"
    )
    def test_invalid_filenames_retry_without_repeating_validated_answers(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "saved.toml"
            target.write_bytes(b"original")
            answers = iter(
                [
                    "n",
                    _URL,
                    "weather.db",
                    "8080",
                    "bad\x00.toml",
                    "bad\ud800.toml",
                    str(target),
                ]
            )

            def prompt(_message: str) -> str:
                self.assertEqual(target.read_bytes(), b"original")
                return next(answers)

            with (
                patch.object(configuration, "_CURRENT_PATH", Path(directory)),
                patch("builtins.input", side_effect=prompt) as inputs,
                patch("builtins.print") as output,
            ):
                created = configuration.create_config_file(
                    None, overwrite_existing=True
                )
            self.assertEqual(created, target)
            self.assertEqual(inputs.call_count, 7)
            self.assertEqual(load_config(created).port, 8080)
            diagnostics = [str(call.args[0]) for call in output.call_args_list][2:]
            self.assertEqual(len(diagnostics), 2)
            self.assertIn("NUL", diagnostics[0])
            self.assertIn("cannot be encoded", diagnostics[1])

    def test_write_failure_retries_only_output_with_the_same_rendered_toml(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            blocked = Path(directory) / "blocked.toml"
            target = Path(directory) / "saved.toml"
            original_write = Path.write_text

            def write(path: Path, data: str, *, encoding: str) -> int:
                if path == blocked:
                    raise PermissionError("blocked \udcff")
                return original_write(path, data, encoding=encoding)

            with (
                patch.object(configuration, "_CURRENT_PATH", Path(directory)),
                patch(
                    "builtins.input",
                    side_effect=[
                        "n",
                        _URL,
                        "weather.db",
                        "8080",
                        str(blocked),
                        str(target),
                    ],
                ) as inputs,
                patch("builtins.print") as output,
                patch.object(
                    Path, "write_text", autospec=True, side_effect=write
                ) as writes,
                patch.object(
                    configuration,
                    "_toml_string",
                    wraps=configuration._toml_string,  # noqa: SLF001
                ) as encode,
            ):
                created = configuration.create_config_file(None)
            self.assertEqual(created, target)
            self.assertEqual(inputs.call_count, 6)
            self.assertEqual(encode.call_count, 2)
            self.assertEqual(writes.call_count, 2)
            self.assertEqual(
                writes.call_args_list[0].args[1], writes.call_args_list[1].args[1]
            )
            self.assertIn(
                r"Could not save configuration: blocked \udcff",
                output.call_args.args[0],
            )
            self.assertFalse(blocked.exists())
            loaded = load_config(created)
            self.assertEqual(
                (loaded.live_data_url, loaded.database_path, loaded.port),
                (_URL, "weather.db", 8080),
            )

    def test_strict_output_escapes_invalid_directory_names_and_allows_replacement(
        self,
    ) -> None:
        for character in ("\udcff", "\ud800"):
            with (
                self.subTest(character=ascii(character)),
                tempfile.TemporaryDirectory() as directory,
            ):
                target = Path(directory) / "saved.toml"
                buffer = io.BytesIO()
                with io.TextIOWrapper(
                    buffer, encoding="utf-8", errors="strict"
                ) as output:
                    with (
                        patch.object(
                            configuration, "_CURRENT_PATH", Path(directory) / character
                        ),
                        patch("sys.stdout", output),
                        patch(
                            "sys.stdin",
                            io.StringIO(f"n\n{_URL}\nweather.db\n\n\n{target}\n"),
                        ),
                    ):
                        created = configuration.create_config_file(None)
                    output.flush()
                    display = buffer.getvalue().decode("utf-8")
                self.assertEqual(created, target)
                self.assertIn("Default database path is unavailable:", display)
                if character == "\udcff":
                    self.assertIn(r"\udcff/aw2sqlite.toml", display)
                else:
                    self.assertIn("Default output filename is unavailable:", display)
                self.assertNotIn(character, display)
                self.assertEqual(load_config(created).database_path, "weather.db")

    @skipUnless(os.name == "posix", "POSIX preserves surrogateescaped filename bytes")
    def test_native_surrogateescaped_filename_is_not_rewritten(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "native_\udcff.toml"
            encoded = os.fsencode(target)
            self.assertEqual(encoded, os.fsencode(directory) + b"/native_\xff.toml")
            with (
                patch("builtins.input", side_effect=["n", _URL, "weather.db", ""]),
                patch.object(
                    Path, "write_text", autospec=True, return_value=1
                ) as write,
                patch(
                    "sys.stdout",
                    io.TextIOWrapper(io.BytesIO(), encoding="utf-8", errors="strict"),
                ),
            ):
                main(["config", "--config", str(target)])
            self.assertEqual(write.call_args.args[0], target)

    @skipUnless(sys.platform == "linux", "Linux supports undecodable POSIX filenames")
    def test_real_native_directory_supports_default_output_filename(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            native = Path(directory) / "native_\udcff"
            native.mkdir()
            with (
                patch.object(configuration, "_CURRENT_PATH", native),
                patch("builtins.input", side_effect=["n", _URL, "weather.db", "", ""]),
                patch(
                    "sys.stdout",
                    io.TextIOWrapper(io.BytesIO(), encoding="utf-8", errors="strict"),
                ),
            ):
                created = configuration.create_config_file(None)
            self.assertEqual(created, native / "aw2sqlite.toml")
            self.assertEqual(load_config(created).database_path, "weather.db")

    def test_existing_prompted_file_keeps_overwrite_policy(self) -> None:
        for overwrite in (False, True):
            with (
                self.subTest(overwrite=overwrite),
                tempfile.TemporaryDirectory() as directory,
            ):
                target = Path(directory) / "existing.toml"
                target.write_bytes(b"original")
                with (
                    patch.object(configuration, "_CURRENT_PATH", Path(directory)),
                    patch(
                        "builtins.input",
                        side_effect=["n", _URL, "weather.db", "", str(target)],
                    ),
                    patch("builtins.print"),
                ):
                    created = configuration.create_config_file(
                        None, overwrite_existing=overwrite
                    )
                self.assertEqual(created, target)
                if overwrite:
                    self.assertEqual(load_config(created).database_path, "weather.db")
                else:
                    self.assertEqual(target.read_bytes(), b"original")

    def test_explicit_nul_filename_fails_before_prompting_or_filesystem_access(
        self,
    ) -> None:
        with (
            patch("builtins.input") as inputs,
            patch.object(Path, "exists") as exists,
            patch.object(Path, "write_text") as write,
            patch("sys.stderr", new_callable=io.StringIO) as errors,
            self.assertRaises(SystemExit) as raised,
        ):
            main(["config", "--config", "bad\x00.toml"])
        self.assertEqual(raised.exception.code, 1)
        self.assertIn(
            "Configuration setup failed: output filenames must not contain NUL",
            errors.getvalue(),
        )
        for activity in (inputs, exists, write):
            activity.assert_not_called()

    def test_explicit_write_failure_reports_once_without_replacement_prompt(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "config.toml"
            with (
                patch(
                    "builtins.input", side_effect=["n", _URL, "weather.db", ""]
                ) as inputs,
                patch.object(
                    Path, "write_text", side_effect=PermissionError("blocked \udcff")
                ),
                patch(
                    "sys.stderr",
                    io.TextIOWrapper(io.BytesIO(), encoding="utf-8", errors="strict"),
                ) as errors,
                patch("builtins.print", wraps=print) as output,
                self.assertRaises(SystemExit) as raised,
            ):
                main(["config", "--config", str(target)])
            self.assertEqual(raised.exception.code, 1)
            self.assertEqual(inputs.call_count, 4)
            diagnostic = output.call_args
            self.assertIn(
                r"Configuration setup failed: blocked \udcff", diagnostic.args[0]
            )
            self.assertIs(diagnostic.kwargs["file"], errors)

    @skipUnless(os.name == "posix", "POSIX rejects high surrogates in filenames")
    def test_explicit_unencodable_filename_fails_before_prompting(self) -> None:
        with (
            patch("builtins.input") as inputs,
            patch.object(Path, "exists") as exists,
            patch.object(Path, "write_text") as write,
            patch("sys.stderr", new_callable=io.StringIO) as errors,
            self.assertRaises(SystemExit) as raised,
        ):
            main(["config", "--config", "bad\ud800.toml"])
        self.assertEqual(raised.exception.code, 1)
        self.assertIn(
            "Configuration setup failed: output filename cannot be encoded",
            errors.getvalue(),
        )
        for activity in (inputs, exists, write):
            activity.assert_not_called()

    def test_configuration_loading_error_display_escapes_surrogate_paths(self) -> None:
        with (
            patch(
                "ambientweather2sqlite.cli.commands.create_config_file",
                return_value=Path("native_\udcff.toml"),
            ),
            patch(
                "ambientweather2sqlite.cli.commands.load_config",
                side_effect=ValueError("bad \udcff"),
            ),
            patch(
                "sys.stderr",
                io.TextIOWrapper(io.BytesIO(), encoding="utf-8", errors="strict"),
            ),
            patch("builtins.print", wraps=print) as output,
            self.assertRaises(SystemExit),
        ):
            main(["serve", "--config", "native.toml"])
        self.assertIn(
            r"Configuration error in native_\udcff.toml: bad \udcff",
            output.call_args.args[0],
        )

    def test_unexpected_setup_errors_and_interrupts_propagate(self) -> None:
        for failure in (RuntimeError("unexpected"), KeyboardInterrupt()):
            with (
                self.subTest(failure=type(failure).__name__),
                patch(
                    "ambientweather2sqlite.cli.commands.create_config_file",
                    side_effect=failure,
                ),
                self.assertRaises(type(failure)) as raised,
            ):
                main(["config"])
            self.assertIs(raised.exception, failure)


class TestStationSelection(TestCase):
    @given(choice=st.text(max_size=100), count=st.integers(min_value=2, max_value=25))
    @example(choice="²", count=2)
    @example(choice="٢", count=2)
    @example(choice="２", count=2)  # noqa: RUF001
    @example(choice="-1", count=2)
    @example(choice="+1", count=2)
    @example(choice="0", count=2)
    @example(choice="99999", count=2)
    @example(choice="9" * 5000, count=2)
    @example(choice="0" * 5000 + "2", count=2)
    def test_choices_are_bounded_ascii_decimal_numbers(
        self, choice: str, count: int
    ) -> None:
        stations = [
            f"http://192.0.2.{number}/livedata.htm" for number in range(1, count + 1)
        ]
        allowed = {str(number): station for number, station in enumerate(stations, 1)}
        expected = allowed.get(choice.strip().lstrip("0"))
        with (
            patch("builtins.input", side_effect=[choice, "1"]) as inputs,
            patch("builtins.print"),
        ):
            result = configuration._prompt_for_discovered_station(stations)  # noqa: SLF001
        self.assertEqual(result, expected if expected is not None else stations[0])
        self.assertEqual(inputs.call_count, 1 if expected is not None else 2)
