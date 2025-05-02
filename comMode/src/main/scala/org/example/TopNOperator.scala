package org.example


trait TopNOperator extends Constant {
  /**
   * @param key       键值
   * @param increment 给成员增加的值
   * @param member    成员名
   * @author
   * @version 2024/5/12 21:53
   * @return
   * @throws
   */
  def addElem(key: String, increment: Double, member: String): Unit

  /**
   * @param key       键值
   * @param start     起始查询索引
   * @param end       终止查询索引
   * @param enableAcs 是否启用升序查询
   * @author
   * @version 2024/5/12 21:55
   * @return 排序后的
   * @throws
   */
  def getTopN(key: String, start: Int, end: Int, enableAcs: Boolean): Array[(String, Double)]
    /**
        * @param key 初始化的键
        * @author
        * @version 2024/5/15 13:55
        * @return
        * @throws
        */
  def cleanElem(key: String): Unit
}
