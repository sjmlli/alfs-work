# ALFS — Anushiravan-Level Fair Scheduler (User-space)

ALFS یک **شبیه‌ساز زمان‌بند (scheduler) در فضای کاربر** است که از ایده‌های
Linux **CFS (Completely Fair Scheduler)** الهام گرفته،  
اما به‌جای **RB-Tree** از **Min-Heap** استفاده می‌کند.

> ⚠️ این پروژه **کرنل نیست**.  
> هدف آن شبیه‌سازی مفاهیم کلیدی CFS (مانند `vruntime`، وزن، nice و fairness)
> به‌صورت **deterministic** در user-space است.

---

## ویژگی‌ها

- پیاده‌سازی user-space از مفاهیم CFS
- استفاده از **Min-Heap** به‌جای RB-Tree
- پشتیبانی از **چند CPU**
- پشتیبانی از **cgroup scheduling**
- پشتیبانی از **CPU affinity**
- پشتیبانی از **CPU burst**
- ارتباط از طریق **Unix Domain Socket (UDS)**
- ورودی و خروجی **JSON**
- خروجی متادیتا (preemptions, migrations)

---

## Build

```bash
make
