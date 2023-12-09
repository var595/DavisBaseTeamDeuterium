class static(object):
    def __new__(cls, *args, **kwargs):
        raise RuntimeError("%s new keyword invalid, static class" % cls)


class ConfigManager(static):
    _prompt = "davisql> "
    _db_name = "DavisBase"
    _data_dir = "data"
    _exec_path = ""

    _tbl_ext = ".tbl"
    _ndx_ext = ".ndx"

    _is_exit = False
    _page_size = 512

    @staticmethod
    def line(rep_s: str, n_reps: int) -> str:
        return "".join([rep_s] * n_reps)

    @classmethod
    def set_exec_path(cls, value: str):
        cls._exec_path = value

    @classmethod
    def set_data_dir(cls, value: str):
        cls._data_dir = value

    @classmethod
    def set_db_name(cls, value: str):
        cls._db_name = value

    @classmethod
    def set_tbl_ext(cls, value: str):
        cls._tbl_ext = value

    @classmethod
    def set_ndx_ext(cls, value: str):
        cls._idx_ext = value

    @classmethod
    def get_prompt(cls, value: str):
        cls._prompt = value

    @classmethod
    def set_exit(cls, value: bool):
        cls._is_exit = value

    @classmethod
    def get_exec_path(cls) -> str:
        return cls._exec_path

    @classmethod
    def get_data_dir(cls) -> str:
        return cls._data_dir

    @classmethod
    def get_db_file(cls) -> str:
        return cls._db_file

    @classmethod
    def get_db_name(cls) -> str:
        return cls._db_name

    @classmethod
    def get_tbl_ext(cls) -> str:
        return cls._tbl_ext

    @classmethod
    def get_ndx_ext(cls) -> str:
        return cls._ndx_ext

    @classmethod
    def get_prompt(cls) -> str:
        return cls._prompt

    @classmethod
    def is_exit(cls) -> bool:
        return cls._is_exit

    @classmethod
    def get_page_size(cls) -> int:
        return cls._page_size
