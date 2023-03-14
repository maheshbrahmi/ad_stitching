. venv/bin/activate
deactivate
zip -g ../convert_to_adori.zip app.py


to create .venv

python3 -m venv venv

1. Run:
	. venv/bin/activate  
	Windows .\venv\Scripts\activate

2. Go to the adori directory and download latest from server
	cd adori_auto_tag
	git pull origin master

3. Install all the requirements:
	pip install -r requirements.txt


to create requirements.txt
$ pip freeze > requirements.txt

to run pip install -r requirements.txt



Deploy zip file from inside convert_to_adori directory

. venv/bin/activate

zip -g ../convert_to_adori.zip app.py
```

