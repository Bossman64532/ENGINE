import ieee_2030_5.client.client 

from ieee_2030_5.client.client import IEEE2030_5_Client


# Certificate paths
CA   = "/home/engine/tls/certs/ca.crt"
CERT = "/home/engine/tls/certs/dev1.crt"        # dev1 certificate
KEY  = "/home/engine/tls/private/dev1.pem"   # dev1 private key

# Create client
client = IEEE2030_5_Client(
   cafile=CA,
   server_hostname="192.168.92.131",
   keyfile=KEY,
   certfile=CERT,
   server_ssl_port=8443,
   debug=True
)

print("\n=== DEVICE CAPABILITY ===")
dcap = client.device_capability("/dcap")
print(dcap)

print("\n=== TIME ===")
print(client.time())

print("\n=== END DEVICES ===")
edev_list = client.end_devices()
for ed in edev_list.EndDevice:
   print("sFDI:", ed.sFDI, "href:", ed.href)

client.disconnect()

