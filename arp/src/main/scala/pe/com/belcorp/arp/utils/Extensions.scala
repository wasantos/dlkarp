package pe.com.belcorp.arp.utils

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, ColumnName, DataFrame}

object Extensions {
  private val counter = new AtomicLong(0)

  implicit class StringToColumn(val sc: StringContext) {
    def $(args: Any*): ColumnName = {
      new ColumnName(sc.s(args: _*))
    }
  }

  implicit class DataFrameExtensions(val df: DataFrame) {
    def exists: Boolean = df.head(1).nonEmpty

    def updateFrom(right: DataFrame,
       joinColumns: Seq[String], valueColumns: Map[String, String]): DataFrame = {
      val count = counter.getAndIncrement()
      val leftAlias = s"left_$count"
      val rightAlias = s"right_$count"

      val leftDf = df.alias(leftAlias)
      val rightDf = df.alias(rightAlias)

      val conditions = joinColumns.foldLeft(lit(true)) { (cond, name) =>
        cond && $"$leftAlias.$name" === $"$rightAlias.$name"
      }

      val projections = Seq($"$leftAlias.*") ++ valueColumns.map { case (l, r) =>
        coalesce($"$rightAlias.$r", $"$leftAlias.$l").as(s"${l}___2")
      }

      val renames = valueColumns.keys.map { l => (l, s"${l}___2") }

      val base = leftDf.join(rightDf, conditions, "left")
        .select(projections: _*)

      renames.foldLeft(base) { (df, t) =>
        df.withColumn(t._1, col(t._2)).drop(t._2)
      }
    }

    def updateFrom(right: DataFrame,
                   joinColumns: Seq[String], valueColumns: String*): DataFrame = {
      updateFrom(right, joinColumns, Map(valueColumns.map(s => (s, s)): _*))
    }

    def updateWith(baseColumns: Seq[Column], valueColumns: Map[String, Column]): DataFrame = {
      val projections = baseColumns ++ valueColumns.map { case (l, r) =>
        r.as(s"${l}___2")
      }

      val renames = valueColumns.keys.map { l => (l, s"${l}___2") }
      val base = df.select(projections: _*)

      renames.foldLeft(base) { (df, t) =>
        df.withColumn(t._1, col(t._2)).drop(t._2)
      }
    }
  }

  def makeEquiJoin(right: String, left: String, columns: Seq[String]): Column = {
    columns.foldLeft(lit(true)) { (cond, name) =>
      cond && $"$left.$name" === $"$right.$name"
    }
  }
}
