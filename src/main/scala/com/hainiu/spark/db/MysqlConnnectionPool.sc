import java.sql.{Connection, DriverManager}

import com.hainiu.spark.config.MyConfig
import org.apache.commons.pool2.{BasePooledObjectFactory, PooledObject}
import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPool}


object MysqlConnnectionPool{
  private val pool = new GenericObjectPool[Connection](new MysqlConnnectionFactory(
    MyConfig.MYSQL_CONFIG("url"),
    MyConfig.MYSQL_CONFIG("userName"),
    MyConfig.MYSQL_CONFIG("password"),
    MyConfig.MYSQL_CONFIG("className")))

  def getConnection: Connection = {
    pool.borrowObject()
  }

}

class MysqlConnnectionFactory(url: String, userName: String, password: String, className: String)
  extends BasePooledObjectFactory[Connection]{
  override def create():Connection = {
    Class.forName(className)
    DriverManager.getConnection(url,userName,password)
  }
  override def wrap(conn:Connection):PooledObject[Connection] = {
    new DefaultPooledObject[Connection](conn)
  }

  override def validateObject(p: PooledObject[Connection]): Boolean = !p.getObject.close()

  override def destroyObject(p: PooledObject[Connection]): Unit = p.getObject.close()
}