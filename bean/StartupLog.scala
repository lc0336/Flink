package com.atguigu.fink.bean

import java.sql.Date
import java.text.SimpleDateFormat

case class StartupLog(mid: String,
                      uid: String,
                      appId: String,
                      area: String,
                      os: String,
                      channel: String,
                      logType: String,
                      version: String,
                      ts: Long) {

  private val date = new Date(ts)

  val logDate: String = new SimpleDateFormat("yyyy-MM-dd").format(date)

  val logHour: String = new SimpleDateFormat("HH").format(date)

  val logHourMinute: String = new SimpleDateFormat("HH:mm").format(date)


  override def hashCode(): Int = uid.hashCode

  override def equals(obj: scala.Any): Boolean = {
    val log: StartupLog = obj.asInstanceOf[StartupLog]
    this.uid == log.uid
  }
}