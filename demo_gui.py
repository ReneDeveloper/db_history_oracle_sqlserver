import pyautogui

TXT_TITULO1="DATABASE AGE: ORACLE SERVERS";TXT_TITULO2="ORACLE CONNECTION PARAMETERS"
OPT_001 = "Generar Metadata";OPT_002 = "Obtener historia de un server";OPT_CLOSE = "Cerrar";

# Crear el mensaje.
opt = pyautogui.confirm(
    "Seleccione una opción",
    "DATABASE AGE: ORACLE SERVERS",
    [OPT_001, OPT_002,OPT_CLOSE]
)
# Tomar decisión en base al botón presionado.
if opt == OPT_CLOSE:
    # ...
    print("OPT_CLOSE")
elif opt == OPT_001:
    # ...
    print("OPT_001")
    ORACLE_SERVER = pyautogui.prompt("Ingrese [SERVERNAME]", TXT_TITULO2,default="SERVERNAME", timeout=30000)
    ORACLE_PORT = pyautogui.prompt("Ingrese [ORACLE_PORT]", TXT_TITULO2,default="1521", timeout=30000)
    ORACLE_SID = pyautogui.prompt("Ingrese [SID]", TXT_TITULO2,default="INSTANCE", timeout=30000)
    ORACLE_USER = pyautogui.prompt("Ingrese [USERNAME]", TXT_TITULO2,default="USER", timeout=30000)
    ORACLE_PW = pyautogui.prompt("Ingrese [PW]", TXT_TITULO2,default="PASSWORK", timeout=30000)

    print(f"ORACLE_SERVER:{ORACLE_SERVER}")
    print(f"ORACLE_SID:{ORACLE_SID}")
    print(f"ORACLE_USER:{ORACLE_USER}")
    print(f"ORACLE_PW:{ORACLE_PW}")

elif opt == OPT_002:
    # ...
    print("OPT_002")

