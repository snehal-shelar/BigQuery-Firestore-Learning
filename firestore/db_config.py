import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from firebase import firebase
import dotenv

# env = load_dotenv()

cred = credentials.Certificate('/home/snehal/Downloads/patients-simulated-data-firebase-adminsdk-fbsvc-3abf6fdb69.json')
default_app = firebase_admin.initialize_app(cred)
db_client = firestore.client()
firebase = firebase.FirebaseApplication('https://patients-simulated-data-default-rtdb.firebaseio.com/', None)
