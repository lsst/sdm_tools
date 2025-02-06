from documenteer.conf.pipelinespkg import *  # noqa: F403, import *

project = "sdm_tools"
html_theme_options["logotext"] = project  # noqa: F405, unknown name
html_title = project
html_short_title = project
doxylink = {}
exclude_patterns = ["changes/*"]

intersphinx_mapping["lsst"] = ("https://pipelines.lsst.io/v/weekly/", None)  # noqa: F405
intersphinx_mapping["pydantic"] = ("https://docs.pydantic.dev/latest/", None)  # noqa: F405
