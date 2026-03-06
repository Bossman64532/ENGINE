from ieee_2030_5.client.client import IEEE2030_5_Client

CA   = "/home/hyejoon-lee/tls/certs/ca.crt"
PEM  = "/home/hyejoon-lee/tls/combined/dev1-combined.pem"

client = IEEE2030_5_Client(
    cafile=CA,
    server_hostname="192.168.40.128",
    certfile=PEM,
    keyfile=PEM,
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
