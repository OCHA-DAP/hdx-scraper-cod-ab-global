"""HXL header rows for the global p-codes and p-code-lengths CSVs."""

ADMIN_2 = 2

headers_pcodes = {
    "Location": ["#country+code"],
    "Admin Level": ["#geo+admin_level"],
    "P-Code": ["#adm+code"],
    "Name": ["#adm+name"],
    "Parent P-Code": ["#adm+code+parent"],
    "Valid from date": ["#date+start"],
    "Version": ["#meta+version"],
}

headers_lengths = {
    "Location": ["#country+code"],
    "Country Length": ["#country+len"],
    "Admin 1 Length": ["#adm1+len"],
    "Admin 2 Length": ["#adm2+len"],
    "Admin 3 Length": ["#adm3+len"],
    "Admin 4 Length": ["#adm4+len"],
    "Admin 5 Length": ["#adm5+len"],
}
