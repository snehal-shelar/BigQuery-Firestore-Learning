import firebase_admin
from firebase import firebase
from firebase_admin import credentials
from google.cloud import firestore  # Provided by google cloud or gcp


# from models import settings
# firebase_admin will be used if you want to use all firebase services. e.g firebase auth and etc.
# from firebase_admin import firestore # Provided by firebase platform
# creds = credentials.Certificate(settings.FIREBASE_CREDENTIALS_PATH)
# default_app = firebase_admin.initialize_app(creds)
# firebase = firebase.FirebaseApplication(settings.FIREBASE_APPLICATION_URL)


# db_client = firestore.Client(database="organizations-fs")
# If your database ID is NOT "(default)", you must specify it:
db_client = firestore.Client(project="dataflow-pipeline-485105", database="organizations-fs")