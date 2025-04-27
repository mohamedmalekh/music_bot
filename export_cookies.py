import browser_cookie3

# Chemin de la base de cookies Brave pour le profil “Default”
BRAVE_COOKIES_DB = r"C:\Users\moham\AppData\Local\BraveSoftware\Brave-Browser\User Data\Default\Cookies"

# On lit les cookies YouTube
cj = browser_cookie3.chrome(domain_name='youtube.com', cookie_file=BRAVE_COOKIES_DB)

# On écrit au format Netscape
with open('cookies.txt', 'w', encoding='utf-8') as f:
    f.write('# Netscape HTTP Cookie File\n')
    for c in cj:
        f.write('\t'.join([
            c.domain,
            'TRUE' if c.domain_specified else 'FALSE',
            c.path,
            'TRUE' if c.secure else 'FALSE',
            str(c.expires or 0),
            c.name,
            c.value
        ]) + '\n')

print("✅ cookies.txt généré.")
