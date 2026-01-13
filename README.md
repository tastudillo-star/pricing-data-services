# pricing-data-services


Opción A (recomendada): symlink (pro, limpio, versionable)

Así editas los archivos en tu repo y systemd los usa.

sudo ln -s \
  /srv/pricing-data-services/etl/prices-gregario/pricing-prices-gregario-etl.service \
  /etc/systemd/system/pricing-prices-gregario-etl.service

sudo ln -s \
  /srv/pricing-data-services/etl/prices-gregario/pricing-prices-gregario-etl.timer \
  /etc/systemd/system/pricing-prices-gregario-etl.timer


Ventaja:

No duplicas archivos


Buenos días, señor. Confirmación clara y directa:

## **Sí.**

👉 **Los `.service` y `.timer` deben estar en `/etc/systemd/system/`** (o enlazados ahí) para que **systemd los reconozca y ejecute**.

Tu carpeta `prices-gregario/` **no es un lugar válido para que systemd lea units automáticamente**.

---

## Opción A (recomendada): **symlink** (pro, limpio, versionable)

Así editas los archivos en tu repo y systemd los usa.

```bash
sudo ln -s \
  /srv/pricing-data-services/etl/prices-gregario/pricing-prices-gregario-etl.service \
  /etc/systemd/system/pricing-prices-gregario-etl.service

sudo ln -s \
  /srv/pricing-data-services/etl/prices-gregario/pricing-prices-gregario-etl.timer \
  /etc/systemd/system/pricing-prices-gregario-etl.timer
```

Ventaja:

* No duplicas archivos
* Cambios en git → impacto inmediato (tras `daemon-reload`)

---

## Opción B: **copiar** (válido, pero menos elegante)

```bash
sudo cp /srv/pricing-data-services/etl/prices-gregario/pricing-prices-gregario-etl.service /etc/systemd/system/
sudo cp /srv/pricing-data-services/etl/prices-gregario/pricing-prices-gregario-etl.timer   /etc/systemd/system/
```

Desventaja:

* Tienes **dos fuentes de verdad**
* Fácil olvidar actualizar `/etc/systemd/system/`

---

## Luego **SIEMPRE**:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now pricing-prices-gregario-etl.timer
```

Verificación:

```bash
systemctl list-timers | grep gregario
systemctl status pricing-prices-gregario-etl.timer
```

---

## Regla mental simple (para todos tus servicios pricing)

* **Código / configs** → `/srv/pricing-data-services/**`
* **Units systemd** → `/etc/systemd/system/`
* **Timers disparan services**
* **Nunca ejecutar ETLs con `cron`**

Si quieres, en el siguiente mensaje puedo:

* revisar tus `.service` / `.timer` línea por línea, o
* darte una **plantilla estándar “pricing-etl”** para que copies y pegues en todos tus ETLs sin pensar.
