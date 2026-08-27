export default {
  async fetch() {
    return Response.json({ ready: true });
  },

  async scheduled(controller) {
    console.log(JSON.stringify({
      event: "scheduled-acceptance",
      cron: controller.cron,
      scheduled_time: controller.scheduledTime,
    }));
  },
};
