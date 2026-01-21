import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from firebase import firebase
from models import settings

creds = credentials.Certificate(settings.FIREBASE_CREDENTIALS_PATH)
default_app = firebase_admin.initialize_app(creds)
db_client = firestore.client()
firebase = firebase.FirebaseApplication(settings.FIREBASE_APPLICATION_URL)
