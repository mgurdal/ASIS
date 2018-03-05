## Planet API Consumer

### Setup
 ** Dependencies:**
 - numpy
 - matplotlib
 - requests
 - rasterio
 - **libgdal-dev**

You can install all the dependencies manually using `pip` or
if you have `pipenv` installed in your machine, you can create a virtual environment with it.
You can install pipenv with `pip install pipenv`.

**Build Steps:**
 - `apt install libgdal-dev -y`
 - `pipenv --three` # create a virtual environment
 - `pipenv install --skip-lock` # install dependencies
 - `pipenv run python planet_api.py`


### Notes

- Use `exit` instead of `deactive` to get outside of virtualenv
- If you want to remove the virtualenv you created before use `pipenv --rm`
- If you want to test the project with **basic.geojson** file you need to make sure you defined your token inside the `planet_api.py`
- or exported like `export PL_API_KEY="your_key"`


### Docker

**Build**
  - `docker build -t planet_client .`

**Run**
  - `docker run -it -e PL_API_KEY=$PL_API_KEY planet_client`

**Execute**
- `pipenv run python planet_api.py`
