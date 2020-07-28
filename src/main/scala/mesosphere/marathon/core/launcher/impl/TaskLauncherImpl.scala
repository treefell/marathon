package mesosphere.marathon
package core.launcher.impl

import java.util.Collections

import com.typesafe.scalalogging.StrictLogging
import mesosphere.marathon.core.launcher.{InstanceOp, TaskLauncher}
import mesosphere.marathon.metrics.Metrics
import org.apache.curator.framework.recipes.locks.Revocable
import org.apache.mesos.Protos.Offer.Operation
import org.apache.mesos.Protos.Resource.RevocableInfo

import scala.jdk.CollectionConverters._
import org.apache.mesos.Protos.{OfferID, Status}
import org.apache.mesos.{Protos, SchedulerDriver}

private[launcher] class TaskLauncherImpl(metrics: Metrics, marathonSchedulerDriverHolder: MarathonSchedulerDriverHolder)
    extends TaskLauncher
    with StrictLogging {

  private[this] val usedOffersMetric =
    metrics.counter("mesos.offers.used")
  private[this] val launchedTasksMetric =
    metrics.counter("tasks.launched")
  private[this] val declinedOffersMetric =
    metrics.counter("mesos.offers.declined")

  def operationWithRevocable(operation: Operation): Operation = {
    //val resourcesBuilder= operation.getLaunch.getTaskInfos(4).getResourcesList.iterator()

    val launchBuilder = operation.getLaunch.toBuilder
    launchBuilder.clearTaskInfos()
    //can make a function of it
    val taskInfoIt = operation.getLaunch.getTaskInfosList.iterator()
    while (taskInfoIt.hasNext){
      val taskInfoBuilder = taskInfoIt.next().toBuilder
      //can make a function of it
      val resIt = taskInfoBuilder.getResourcesList.iterator()
      taskInfoBuilder.clearResources()
      while (resIt.hasNext) {
        if (resIt.next().getName == "cpus")
          taskInfoBuilder.addResources(resIt.next().toBuilder.setRevocable(RevocableInfo.newBuilder()).build())
        else
          taskInfoBuilder.addResources(resIt.next())
      }
      launchBuilder.addTaskInfos(taskInfoBuilder.build())
    }

    val operationBuilder = operation.toBuilder
    operationBuilder.setLaunch(launchBuilder)
    logger.info(s"DEBUG OPPORTUNISTIC:\n${operationBuilder.getLaunch.getTaskInfosList}\n" )
    operationBuilder.build()
  }

  override def acceptOffer(offerID: OfferID, taskOps: Seq[InstanceOp]): Boolean = {
    val accepted = withDriver(s"launchTasks($offerID)") { driver =>
      //We accept the offer, the rest of the offer is declined automatically with the given filter.
      //The filter duration is set to 0, so we get the same offer in the next allocator cycle.
      val noFilter = Protos.Filters.newBuilder().setRefuseSeconds(0).build()
      val operations = taskOps.flatMap(_.offerOperations)
      logger.info(s"Operations on $offerID:\n${operations.mkString("\n")}")
      val newOperations = operations.map(operationWithRevocable)
      driver.acceptOffers(Collections.singleton(offerID), newOperations.asJava, noFilter)
    }
    if (accepted) {
      usedOffersMetric.increment()
      val launchCount = taskOps.count {
        case _: InstanceOp.LaunchTask => true
        case _: InstanceOp.LaunchTaskGroup => true
        case _ => false
      }
      launchedTasksMetric.increment(launchCount.toLong)
    }
    accepted
  }

  override def declineOffer(offerID: OfferID, refuseMilliseconds: Option[Long]): Unit = {
    val declined = withDriver(s"declineOffer(${offerID.getValue})") {
      val filters = refuseMilliseconds
        .map(seconds => Protos.Filters.newBuilder().setRefuseSeconds(seconds / 1000.0).build())
        .getOrElse(Protos.Filters.getDefaultInstance)
      _.declineOffer(offerID, filters)
    }
    if (declined) {
      declinedOffersMetric.increment()
    }
  }

  private[this] def withDriver(description: => String)(block: SchedulerDriver => Status): Boolean = {
    marathonSchedulerDriverHolder.driver match {
      case Some(driver) =>
        val status = block(driver)
        logger.debug(s"$description returned status = $status")

        status == Status.DRIVER_RUNNING

      case None =>
        logger.warn(s"Cannot execute '$description', no driver available")
        false
    }
  }
}
