from AzureProvider import AzureProvider

vm_reference = {
        'publisher': 'Canonical',
        'offer': 'UbuntuServer',
        'sku': '16.04.0-LTS',
        'version': 'latest',
        'vm_size': 'Standard_DS1_v2',
        'disk_size_gb': 10,
        "admin_username": "parsl.auto.admin",
        "password": "@@86*worth*TRUST*problem*69@@"
    }

provider = AzureProvider(
    key_file="azure_keys.json", instance_type_ref=vm_reference)
print(provider.current_capacity)
id = provider.submit()
id2 = provider.submit()
print(provider.current_capacity)
print(provider.status([id, id2]))
provider.cancel([id, id2])
print(provider.current_capacity)