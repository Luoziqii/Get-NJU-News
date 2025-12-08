# Get-NJU-News
A program to get news from the website of NJU
## 使用注意
1. 检查python版本是否3.7左右，由于不想处理ssl握手问题，笨人本地测试时使用了较低版本的3.7，低版本应该都行但阈值未知。
2. 运行前请安装依赖库：pip install -r requirements.txt
3. 运行前请配置config.ini配置文件，修改PASSWORD（数据库密码）与DATABASE_NAME（数据库名，默认nju_news）属性
4. 先运行create_database.py，以创建数据库以及表单，若已有数据库，请将config.ini中DATABASE_NAME更改为已有数据库，则只创建表单。表单名默认为news_all，可根据需求修改。
5. 再运行get_nju_news.py。
6. 目前代码尚不完善，图片与附件未爬取，发生bug也属正常情况，还请包涵。