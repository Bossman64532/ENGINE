#This is a client that should run continuously. It should take inputs to query the server and spit out the results. Change the IP address and port number as needed. It should also perform service discovery on the local network to find the services.
import ieee_2030_5.client.client 
from zeroconf import ServiceBrowser, ServiceInfo, ServiceListener, Zeroconf
from ieee_2030_5.client.client import IEEE2030_5_Client


# Certificate paths
CA   = "/home/engine/tls/certs/ca.crt"
CERT = "/home/engine/tls/certs/dev1.crt"        # dev1 certificate
KEY  = "/home/engine/tls/private/dev1.pem"   # dev1 private key
serverHostname = "192.168.92.131"
# Create client
def make_request(request="/dcap", cafile=CA, server_hostname=serverHostname, keyfile=KEY, certfile=CERT, server_ssl_port=8443, debug=True):
    if request:
        client = IEEE2030_5_Client(
        cafile=cafile,
        server_hostname=server_hostname,
        keyfile=keyfile,
        certfile=certfile,
        server_ssl_port=server_ssl_port,
        debug=debug
        )
        print(client.get(request))
        client.disconnect()


def changeHostname(new_hostname):
    global serverHostname
    serverHostname = new_hostname
    print(f"Server hostname changed to: {serverHostname}")
    

# print("\n=== DEVICE CAPABILITY ===")
# dcap = client.device_capability("/dcap")
# print(dcap)

# print("\n=== TIME ===")
# print(client.time())

# print("\n=== END DEVICES ===")
# edev_list = client.end_devices()
# for ed in edev_list.EndDevice:
#    print("sFDI:", ed.sFDI, "href:", ed.href)

# client.disconnect()
while True:
    user_input = input("Enter a request (or 'exit' to quit, 'change' to change server hostname): ")
    if user_input.lower() == 'exit':
        print("Exiting...")
        break
    elif user_input.lower() == 'change':
        new_hostname = input("Enter new server hostname: ")
        changeHostname(new_hostname)
    else:
        make_request(user_input)