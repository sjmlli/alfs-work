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
خروجی باینری:

./alfs
Run
./alfs --cpus N --quanta-us Q --socket PATH [options]
پارامترها
--cpus N
تعداد CPUها

--quanta-us Q
اندازه کوانتای هر tick (میکروثانیه)

--socket PATH (اختیاری)
مسیر Unix Domain Socket
اگر مشخص نشود، برنامه به‌ترتیب این‌ها را امتحان می‌کند:

./event.socket

./socket.event

--burst-mode MODE (اختیاری)

freeze_pushback (پیش‌فرض)
vruntime = max_vruntime و سپس freeze

freeze
فقط freeze بدون pushback

track
vruntime مثل حالت عادی update می‌شود

--meta-minimal (پیش‌فرض)
فقط { migrations, preemptions }

--meta-extra
شامل:

migrations

preemptions

runnableTasks

blockedTasks

--debug
چاپ لاگ‌های داخلی روی stderr

نمونه اجرا
./alfs --cpus 4 --quanta-us 1000 --socket socket.event --debug
I/O Protocol (UDS + JSON)
برنامه به‌عنوان CLIENT به UDS وصل می‌شود

ورودی: برای هر vtime یک JSON (TimeFrame)

خروجی: برای همان vtime یک JSON از نوع SchedulerTick

هر خروجی با newline خاتمه می‌یابد

Robust JSON Framing
ورودی به‌صورت stream خوانده می‌شود و با روش زیر یک JSON کامل استخراج می‌شود:

شمارش {} و []

در نظر گرفتن string و escape

وقتی depth به صفر برسد → JSON کامل

Data Model
Task
id (string)

nice در بازه [-20, +19]

weight (بر اساس جدول weight_to_prio)

vruntime_us

state : RUNNABLE | BLOCKED | EXITED

cpuMask

cgroup

burst_remaining

burst_freeze

Cgroup
cpuShares

cpuQuotaUs

cpuPeriodUs

cpuMask

vruntime_us

Scheduling Algorithm (CFS-inspired)
معادله vruntime
vruntime += delta_exec * NICE_0_WEIGHT / weight
delta_exec = quanta_us
تسکی که CPU کمتری گرفته باشد، vruntime کمتری دارد و زودتر انتخاب می‌شود.

Heap Structure
دو سطح heap داریم:

heap_cg

کلید: cg->vruntime_us

heap_task (داخل هر cgroup)

کلید: task->vruntime_us

انتخاب task
انتخاب cgroup با کمترین vruntime

انتخاب task با کمترین vruntime در آن cgroup

بررسی:

throttled نباشد

runnable باشد

cpuMask اجازه بدهد

در همان tick روی CPU دیگر انتخاب نشده باشد

Determinism
ترتیب CPUها ثابت است

tie-break:

(vruntime, seq, id)
task انتخاب‌شده تا پایان tick به heap برنمی‌گردد

Event Semantics
CREATE_TASK

BLOCK_TASK

UNBLOCK_TASK

TASK_YIELD

SETNICE_TASK

TASK_SET_AFFINITY

*_CGROUP

CGROUP_MOVE_TASK

BURST_CPU(taskId, duration)

Metadata
preemptions
تعداد tickهایی که task قبل روی CPU بوده و قطع شده

migrations
تعداد تغییر CPU یک task (idle حساب نمی‌شود)

Tests
یک سرور تست ساده وجود دارد:

python3 tests/server.py socket.event tests/trace_demo.jsonl
در ترمینال دیگر:

./alfs --cpus 1 --quanta-us 1000 --meta-minimal --socket socket.event
برای تست چند CPU:

./alfs --cpus 2 --quanta-us 1000 --meta-extra --socket socket.event
Files
src/alfs.c
هسته scheduler + heap + JSON + UDS

third_party/jsmn.h
JSON parser سبک

tests/server.py
سرور تست UDS

tests/trace_demo.jsonl
سناریوی تست

report/REPORT.md
گزارش تحلیلی (RB-Tree vs Heap, EEVDF, big.LITTLE)

Mental Model
CFS تلاش می‌کند CPU را متناسب با وزن تقسیم کند.
vruntime نشان می‌دهد چه کسی کمتر سهم گرفته است.
