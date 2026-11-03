## Alab

alab


## Get started
1. Make sure you are developing inside a virtual environment to manage dependencies properly.
2. Add you MP_API_KEY to environmental variables with 
```
export MP_API_KEY=<YOUR_API_KEY>
```
3. Add your awesome design to: `src/alab/pages/`
4. To query data from the Materials Project API, use:
```
from mpships_infra import get_mp_rester

mpr = get_mp_rester()
docs = mpr.materials.summary.search(material_ids = ["mp-149"])
```

## Useful resourses
- [Crystal Toolkit](https://github.com/materialsproject/crystaltoolkit): a web app framework from the Materials Project allowing Python developers to easily make an interactive web app to display materials science information.