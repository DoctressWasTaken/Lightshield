#./wait-for-it.sh proxy:8000
python manage.py migrate
python -u run.py
